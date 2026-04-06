"""
Data models for the Weather ETL Pipeline.

Why Pydantic?
- Type safety: every field has a declared type
- Validation: bad data raises errors BEFORE touching the database
- Documentation: the model IS the documentation of your data contract
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator


class WeatherRaw(BaseModel):
    """
    Raw weather data exactly as it comes from the API.
    No transformations — just validated structure.
    """
    city: str = Field(..., min_length=1, description="City name")
    country: str = Field(..., min_length=2, max_length=2, description="ISO country code e.g. US")
    recorded_at: datetime = Field(..., description="Timestamp of the reading")
    temperature_kelvin: float = Field(..., description="Temperature in Kelvin from API")
    feels_like_kelvin: float = Field(..., description="Feels like temp in Kelvin")
    humidity_percent: int = Field(..., ge=0, le=100, description="Humidity 0-100")
    pressure_hpa: float = Field(..., gt=0, description="Atmospheric pressure in hPa")
    wind_speed_ms: float = Field(..., ge=0, description="Wind speed in metres per second")
    weather_condition: str = Field(..., description="Main condition e.g. Clear, Rain")
    weather_description: str = Field(..., description="Detailed description")

    @field_validator("city")
    @classmethod
    def city_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("City name cannot be blank")
        return v.strip().title()

    @field_validator("country")
    @classmethod
    def country_must_be_uppercase(cls, v: str) -> str:
        return v.upper()


class WeatherTransformed(BaseModel):
    """
    Cleaned and enriched weather data ready for the database.
    This is what gets loaded — never raw API data directly.
    """
    city: str
    country: str
    recorded_at: datetime
    temperature_celsius: float
    feels_like_celsius: float
    temperature_delta: float        # difference between actual and feels-like
    humidity_percent: int
    pressure_hpa: float
    wind_speed_ms: float
    wind_speed_kmh: float           # derived: ms * 3.6
    weather_condition: str
    weather_description: str
    humidity_category: str          # derived: Low / Moderate / High
    pipeline_run_id: Optional[str] = None
    loaded_at: Optional[datetime] = None

    @field_validator("temperature_celsius", "feels_like_celsius")
    @classmethod
    def temperature_in_valid_range(cls, v: float) -> float:
        if v < -90 or v > 60:
            raise ValueError(f"Temperature {v}C is outside valid Earth range")
        return round(v, 2)

    @field_validator("wind_speed_kmh")
    @classmethod
    def wind_speed_rounded(cls, v: float) -> float:
        return round(v, 2)


class PipelineRun(BaseModel):
    """
    Metadata for every pipeline execution.
    This is how you answer: what ran, when, did it succeed?
    Senior engineers always track pipeline runs.
    """
    run_id: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    status: str = Field(..., pattern="^(running|success|failed)$")
    rows_extracted: int = 0
    rows_transformed: int = 0
    rows_loaded: int = 0
    error_message: Optional[str] = None
    duration_seconds: Optional[float] = None
