import polars as pl
import hashlib
import decimal
from typing import Tuple
from models import setup_logger, FinancialRecord
from pydantic import ValidationError

logger = setup_logger("financial_etl.validator")

class DataValidator:
    def __init__(self, file_path: str):
        self.file_path = file_path

    @staticmethod
    def generate_idempotency_key(row: dict) -> str:
        """Generates SHA-256 idempotency key based on transaction attributes."""
        payload = f"{row.get('timestamp', '')}|{row.get('amount', '')}|{row.get('entry_type', '')}|{row.get('account_code', '')}"
        return hashlib.sha256(payload.encode('utf-8')).hexdigest()

    def process_silver_layer(self, df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        valid_records = []
        quarantine_records = []
        seen_keys = set()

        for row in df.iter_rows(named=True):
            tx_id = row.get("transaction_id", "UNKNOWN")
            
            idem_key = self.generate_idempotency_key(row)
            if idem_key in seen_keys:
                logger.warning("Duplicate detected via idempotency key.", extra={"transaction_id": tx_id})
                row['error'] = "Duplicate idempotency key"
                quarantine_records.append(row)
                continue
            
            seen_keys.add(idem_key)

            try:
                raw_dict = row.copy()
                if 'amount' in raw_dict and raw_dict['amount'] is not None:
                    raw_dict['amount'] = str(raw_dict['amount'])
                
                record = FinancialRecord(**raw_dict)
                valid_dict = record.model_dump()
                # Ensure ISO isoformat string structure for proper polars grouping
                valid_dict['timestamp'] = valid_dict['timestamp'].isoformat()
                valid_records.append(valid_dict)
            except ValidationError as e:
                logger.error("Pydantic Validation Error.", extra={"transaction_id": tx_id, "error": str(e)})
                row['error'] = "Validation/Type Error"
                quarantine_records.append(row)

        valid_df = pl.DataFrame(valid_records) if valid_records else pl.DataFrame()
        quarantine_df = pl.DataFrame(quarantine_records) if quarantine_records else pl.DataFrame()
        
        return valid_df, quarantine_df

    def enforce_accounting_guardrail(self, valid_df: pl.DataFrame) -> bool:
        if valid_df.height == 0:
            return True
            
        try:
            debits_df = valid_df.filter(pl.col("entry_type") == "debit")
            credits_df = valid_df.filter(pl.col("entry_type") == "credit")
            
            if debits_df.height == 0 or credits_df.height == 0:
                logger.error("Accounting guardrail failed: Partial batch missing debit or credit")
                return False

            sum_debits = sum([decimal.Decimal(str(d)) for d in debits_df.select(pl.col("amount")).to_series().to_list()])
            sum_credits = sum([decimal.Decimal(str(c)) for c in credits_df.select(pl.col("amount")).to_series().to_list()])
            
            if sum_debits == sum_credits:
                logger.info("Accounting guardrail passed: debits == credits")
                return True
            else:
                logger.error(f"Accounting guardrail failed. Debits ({sum_debits}) != Credits ({sum_credits})")
                return False
        except Exception as e:
            logger.error(f"Error evaluating balance: {e}")
            return False

    def run_pipeline(self) -> Tuple[pl.DataFrame, pl.DataFrame]:
        logger.info("Starting Bronze layer extraction (Lazy mode)")
        try:
            lazy_df = pl.scan_csv(self.file_path, schema_overrides={"amount": pl.String, "account_code": pl.String})
            bronze_df = lazy_df.collect()
        except Exception as e:
            logger.error(f"Failed to read CSV: {e}")
            return pl.DataFrame(), pl.DataFrame([{"error": str(e)}])

        logger.info("Starting Silver layer processing")
        valid_df, quarantine_df = self.process_silver_layer(bronze_df)

        final_valid_records = []
        final_quarantine_records = quarantine_df.to_dicts() if quarantine_df.height > 0 else []

        if valid_df.height > 0:
            grouped = valid_df.group_by(["timestamp"], maintain_order=True)
            for (timestamp,), group_df in grouped:
                is_balanced = self.enforce_accounting_guardrail(group_df)
                if not is_balanced:
                    logger.error("Batch completely failed accounting guardrail. Quarantining.", extra={"batch_timestamp": timestamp})
                    group_quarantine = group_df.with_columns(pl.lit("Accounting Guardrail Failed").alias("error"))
                    final_quarantine_records.extend(group_quarantine.to_dicts())
                else:
                    final_valid_records.extend(group_df.to_dicts())

        final_valid_df = pl.DataFrame(final_valid_records) if final_valid_records else pl.DataFrame()
        final_quarantine_df = pl.DataFrame(final_quarantine_records) if final_quarantine_records else pl.DataFrame()

        return final_valid_df, final_quarantine_df
