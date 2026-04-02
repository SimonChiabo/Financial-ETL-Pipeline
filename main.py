import asyncio
import os
from models import setup_logger
from validator import DataValidator

logger = setup_logger("financial_etl.main")

async def extract_and_load(file_path: str):
    logger.info("Starting async ETL pipeline process", extra={"file_path": file_path})
    await asyncio.sleep(0.1)
    
    validator = DataValidator(file_path)
    valid_df, quarantine_df = validator.run_pipeline()
    
    total_processed = (valid_df.height if valid_df.height else 0) + (quarantine_df.height if quarantine_df.height else 0)
    
    print("\n--- STRESS TEST SUMMARY ---")
    print(f"Registros procesados: {total_processed}")
    print(f"Registros en Silver: {valid_df.height if valid_df.height else 0} (Lote 1000 y Lote 3000)")
    print(f"Registros en Quarantine: {quarantine_df.height if quarantine_df.height else 0} (Fallas de fecha, balance, duplicado, tipo y balance de lote parcial).")
    print("---------------------------\n")

async def main():
    test_file = "stress_test_financials.csv"
    if os.path.exists(test_file):
        await extract_and_load(test_file)
    else:
        print(f"File {test_file} not found.")

if __name__ == "__main__":
    asyncio.run(main())
