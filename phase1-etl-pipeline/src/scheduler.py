"""
Scheduler and main pipeline orchestrator.
"""
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from src.extractor import WeatherExtractor
from src.transformer import WeatherTransformer
from src.loader import WeatherLoader
from src.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'postgres')}:"
    f"{os.getenv('DB_PASSWORD', 'postgres')}@"
    f"{os.getenv('DB_HOST', '127.0.0.1')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'weather_pipeline')}"
)

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITIES = os.getenv("CITIES", "London,Dallas,Tokyo").split(",")


def run_pipeline():
    started_at = datetime.utcnow()
    logger.info("=" * 50)
    logger.info("PIPELINE RUN STARTING")
    logger.info("=" * 50)

    loader = WeatherLoader(DB_URL)
    loader.create_tables()
    run_id = loader.start_pipeline_run()

    try:
        extractor = WeatherExtractor(api_key=API_KEY)
        raw_records = extractor.extract(CITIES)
        rows_extracted = len(raw_records)

        transformer = WeatherTransformer()
        transformed_records = transformer.transform(raw_records)
        rows_transformed = len(transformed_records)

        rows_loaded = loader.upsert_weather(transformed_records, run_id)

        duration = (datetime.utcnow() - started_at).total_seconds()
        loader.finish_pipeline_run(
            run_id=run_id,
            status="success",
            rows_extracted=rows_extracted,
            rows_transformed=rows_transformed,
            rows_loaded=rows_loaded,
            duration_seconds=duration
        )

        logger.info("=" * 50)
        logger.info(f"PIPELINE SUCCESS in {duration:.2f}s")
        logger.info(f"Extracted: {rows_extracted} | Transformed: {rows_transformed} | Loaded: {rows_loaded}")
        logger.info("=" * 50)

    except Exception as e:
        duration = (datetime.utcnow() - started_at).total_seconds()
        logger.error(f"PIPELINE FAILED: {e}")
        loader.finish_pipeline_run(
            run_id=run_id,
            status="failed",
            error_message=str(e),
            duration_seconds=duration
        )


if __name__ == "__main__":
    logger.info(f"Starting pipeline for cities: {CITIES}")
    run_pipeline()
    logger.info("Pipeline complete. Run again manually or add scheduler.")
