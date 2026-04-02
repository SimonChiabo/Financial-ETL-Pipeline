import uuid
import datetime
import decimal
import json
import logging
from enum import Enum
from pydantic import BaseModel, Field, field_validator

# Configure JSON structured logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_record = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if hasattr(record, "transaction_id"):
            log_record["transaction_id"] = record.transaction_id
        if record.exc_info:
            log_record["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def setup_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

logger = setup_logger("financial_etl.models")

class EntryType(str, Enum):
    DEBIT = "debit"
    CREDIT = "credit"

class FinancialRecord(BaseModel):
    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime.datetime
    amount: decimal.Decimal
    entry_type: EntryType
    account_code: str
    
    @field_validator("account_code")
    @classmethod
    def validate_account_code(cls, v):
        if not v.isalnum() or len(v) < 4:
            raise ValueError("account_code must be alphanumeric and at least 4 characters")
        return v.upper()

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v):
        if isinstance(v, str):
            try:
                # Handle ISO format easily
                return datetime.datetime.fromisoformat(v.replace("Z", "+00:00"))
            except ValueError:
                raise ValueError("timestamp must be in a valid ISO format")
        return v
    
    @field_validator("amount")
    @classmethod
    def validate_amount(cls, v):
        if float(v) < 0:
            raise ValueError("Amount must be non-negative")
        return v
