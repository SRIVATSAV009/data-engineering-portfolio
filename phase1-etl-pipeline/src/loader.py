"""
Database loader for the Weather ETL Pipeline.

Responsibilities:
- Create tables if they don't exist (idempotent setup)
- Upsert weather records (idempotent loads — no duplicates ever)
- Track every pipeline run in a metadata table
- Manage database connections safely
"""

import uuid
from datetime import datetime
from typing import List

from sqlalchemy import (
    create_engine, text,
    Column, String, Float, Integer,
    DateTime, Text
)
from sqlalchemy.orm import declarative_base, Session

from src.logger import get_logger
from src.models import WeatherTransformed, PipelineRun

logger = get_logger(__name__)
Base = declarative_base()


# ── ORM Table Definitions ──────────────────────────────────────────

class WeatherRecord(Base):
    """Maps to the weather_data table in PostgreSQL."""
    __tablename__ = "weather_data"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String(100), nullable=False)
    country = Column(String(2), nullable=False)
    recorded_at = Column(DateTime, nullable=False)
    temperature_celsius = Column(Float, nullable=False)
    feels_like_celsius = Column(Float, nullable=False)
    temperature_delta = Column(Float)
    humidity_percent = Column(Integer)
    pressure_hpa = Column(Float)
    wind_speed_ms = Column(Float)
    wind_speed_kmh = Column(Float)
    weather_condition = Column(String(100))
    weather_description = Column(String(200))
    humidity_category = Column(String(20))
    pipeline_run_id = Column(String(36))
    loaded_at = Column(DateTime, default=datetime.utcnow)


class PipelineRunRecord(Base):
    """Maps to the pipeline_runs table — tracks every execution."""
    __tablename__ = "pipeline_runs"

    run_id = Column(String(36), primary_key=True)
    started_at = Column(DateTime, nullable=False)
    finished_at = Column(DateTime)
    status = Column(String(20), nullable=False)
    rows_extracted = Column(Integer, default=0)
    rows_transformed = Column(Integer, default=0)
    rows_loaded = Column(Integer, default=0)
    error_message = Column(Text)
    duration_seconds = Column(Float)


# ── Loader Class ───────────────────────────────────────────────────

class WeatherLoader:

    def __init__(self, db_url: str):
        self.engine = create_engine(db_url, echo=False)
        logger.info("Database engine created")

    def create_tables(self):
        """
        Creates all tables if they don't exist.
        Safe to run multiple times — idempotent.
        """
        Base.metadata.create_all(self.engine)
        logger.info("Tables created or already exist: weather_data, pipeline_runs")

    def upsert_weather(
        self,
        records: List[WeatherTransformed],
        run_id: str
    ) -> int:
        """
        Inserts or updates weather records.
        ON CONFLICT: if same city + recorded_at exists, update it.
        This makes every pipeline run safe to retry.
        """
        if not records:
            logger.warning("No records to load")
            return 0

        loaded = 0
        with Session(self.engine) as session:
            for rec in records:
                stmt = text("""
                    INSERT INTO weather_data (
                        city, country, recorded_at,
                        temperature_celsius, feels_like_celsius, temperature_delta,
                        humidity_percent, pressure_hpa,
                        wind_speed_ms, wind_speed_kmh,
                        weather_condition, weather_description,
                        humidity_category, pipeline_run_id, loaded_at
                    ) VALUES (
                        :city, :country, :recorded_at,
                        :temperature_celsius, :feels_like_celsius, :temperature_delta,
                        :humidity_percent, :pressure_hpa,
                        :wind_speed_ms, :wind_speed_kmh,
                        :weather_condition, :weather_description,
                        :humidity_category, :pipeline_run_id, :loaded_at
                    )
                    ON CONFLICT (city, recorded_at)
                    DO UPDATE SET
                        temperature_celsius  = EXCLUDED.temperature_celsius,
                        feels_like_celsius   = EXCLUDED.feels_like_celsius,
                        temperature_delta    = EXCLUDED.temperature_delta,
                        humidity_percent     = EXCLUDED.humidity_percent,
                        weather_condition    = EXCLUDED.weather_condition,
                        weather_description  = EXCLUDED.weather_description,
                        humidity_category    = EXCLUDED.humidity_category,
                        pipeline_run_id      = EXCLUDED.pipeline_run_id,
                        loaded_at            = EXCLUDED.loaded_at
                """)
                session.execute(stmt, {
                    "city": rec.city,
                    "country": rec.country,
                    "recorded_at": rec.recorded_at,
                    "temperature_celsius": rec.temperature_celsius,
                    "feels_like_celsius": rec.feels_like_celsius,
                    "temperature_delta": rec.temperature_delta,
                    "humidity_percent": rec.humidity_percent,
                    "pressure_hpa": rec.pressure_hpa,
                    "wind_speed_ms": rec.wind_speed_ms,
                    "wind_speed_kmh": rec.wind_speed_kmh,
                    "weather_condition": rec.weather_condition,
                    "weather_description": rec.weather_description,
                    "humidity_category": rec.humidity_category,
                    "pipeline_run_id": run_id,
                    "loaded_at": datetime.utcnow()
                })
                loaded += 1

            session.commit()

        logger.info(f"Upserted {loaded} records into weather_data")
        return loaded

    def start_pipeline_run(self) -> str:
        """Creates a pipeline_run record with status=running. Returns run_id."""
        run_id = str(uuid.uuid4())
        with Session(self.engine) as session:
            session.execute(text("""
                INSERT INTO pipeline_runs (run_id, started_at, status)
                VALUES (:run_id, :started_at, 'running')
            """), {"run_id": run_id, "started_at": datetime.utcnow()})
            session.commit()
        logger.info(f"Pipeline run started: {run_id}")
        return run_id

    def finish_pipeline_run(
        self,
        run_id: str,
        status: str,
        rows_extracted: int = 0,
        rows_transformed: int = 0,
        rows_loaded: int = 0,
        error_message: str = None,
        duration_seconds: float = None
    ):
        """Updates the pipeline_run record with final status and metrics."""
        with Session(self.engine) as session:
            session.execute(text("""
                UPDATE pipeline_runs SET
                    finished_at      = :finished_at,
                    status           = :status,
                    rows_extracted   = :rows_extracted,
                    rows_transformed = :rows_transformed,
                    rows_loaded      = :rows_loaded,
                    error_message    = :error_message,
                    duration_seconds = :duration_seconds
                WHERE run_id = :run_id
            """), {
                "run_id": run_id,
                "finished_at": datetime.utcnow(),
                "status": status,
                "rows_extracted": rows_extracted,
                "rows_transformed": rows_transformed,
                "rows_loaded": rows_loaded,
                "error_message": error_message,
                "duration_seconds": duration_seconds
            })
            session.commit()
        logger.info(f"Pipeline run {run_id} finished: {status}")
