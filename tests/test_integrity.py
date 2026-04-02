import pytest
import polars as pl
import uuid
import os
import decimal
from validator import DataValidator
from models import FinancialRecord

@pytest.fixture
def test_dir(tmp_path):
    return tmp_path

def create_csv(path, rows):
    df = pl.DataFrame(rows)
    df.write_csv(path)

def test_perfectly_balanced_batch(test_dir):
    file_path = os.path.join(test_dir, "balanced.csv")
    rows = [
        {"transaction_id": str(uuid.uuid4()), "timestamp": "2023-10-01T12:00:00Z", "amount": "100.50", "entry_type": "debit", "account_code": "CASH"},
        {"transaction_id": str(uuid.uuid4()), "timestamp": "2023-10-01T12:05:00Z", "amount": "100.50", "entry_type": "credit", "account_code": "REVENUE"},
    ]
    create_csv(file_path, rows)
    
    validator = DataValidator(file_path)
    valid_df, quarantine_df = validator.run_pipeline()
    
    assert valid_df.height == 2
    assert quarantine_df.height == 0

def test_unbalanced_batch(test_dir):
    file_path = os.path.join(test_dir, "unbalanced.csv")
    rows = [
        {"transaction_id": str(uuid.uuid4()), "timestamp": "2023-10-01T12:00:00Z", "amount": "100.50", "entry_type": "debit", "account_code": "CASH"},
        {"transaction_id": str(uuid.uuid4()), "timestamp": "2023-10-01T12:05:00Z", "amount": "90.00", "entry_type": "credit", "account_code": "REVENUE"},
    ]
    create_csv(file_path, rows)
    
    validator = DataValidator(file_path)
    valid_df, quarantine_df = validator.run_pipeline()
    
    assert valid_df.height == 0
    assert quarantine_df.height == 2
    assert "error" in quarantine_df.columns
    assert "Accounting Guardrail Failed" in quarantine_df.select(pl.col("error")).row(0)

def test_duplicate_record(test_dir):
    file_path = os.path.join(test_dir, "duplicates.csv")
    tx_id1 = str(uuid.uuid4())
    rows = [
        {"transaction_id": tx_id1, "timestamp": "2023-10-01T12:00:00Z", "amount": "100.50", "entry_type": "debit", "account_code": "CASH"},
        {"transaction_id": tx_id1, "timestamp": "2023-10-01T12:00:00Z", "amount": "100.50", "entry_type": "debit", "account_code": "CASH"},
        {"transaction_id": str(uuid.uuid4()), "timestamp": "2023-10-01T12:05:00Z", "amount": "100.50", "entry_type": "credit", "account_code": "REVENUE"},
    ]
    create_csv(file_path, rows)
    
    validator = DataValidator(file_path)
    valid_df, quarantine_df = validator.run_pipeline()
    
    assert valid_df.height == 2
    assert quarantine_df.height == 1
    assert "Duplicate idempotency key" in quarantine_df.select(pl.col("error")).row(0)

def test_precision_financial_amounts(test_dir):
    # Ensure no floating point errors occur with large decimal precision
    file_path = os.path.join(test_dir, "precision.csv")
    rows = [
        # 0.1 + 0.2 = 0.3 trick for floats. Decimals evaluate exactly.
        {"transaction_id": str(uuid.uuid4()), "timestamp": "2023-10-01T12:00:00Z", "amount": "0.1", "entry_type": "debit", "account_code": "CASH"},
        {"transaction_id": str(uuid.uuid4()), "timestamp": "2023-10-01T12:01:00Z", "amount": "0.2", "entry_type": "debit", "account_code": "CASH"},
        {"transaction_id": str(uuid.uuid4()), "timestamp": "2023-10-01T12:02:00Z", "amount": "0.3", "entry_type": "credit", "account_code": "REVENUE"},
    ]
    create_csv(file_path, rows)
    validator = DataValidator(file_path)
    valid_df, quarantine_df = validator.run_pipeline()
    
    # Needs to be balanced! If it used floats internally, 0.1 + 0.2 = 0.30000000000000004 and it would fail the balance
    assert valid_df.height == 3
    assert quarantine_df.height == 0
