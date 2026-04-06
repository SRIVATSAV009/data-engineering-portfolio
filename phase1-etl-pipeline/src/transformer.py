"""
Transformer for the Weather ETL Pipeline.

Responsibilities:
- Convert raw API units to human-friendly units
- Derive new fields (wind_speed_kmh, humidity_category, temperature_delta)
- Validate all transformed data with Pydantic
- Transformation ONLY — no API calls, no DB writes
"""

from typing import List
from src.logger import get_logger
from src.models import WeatherRaw, WeatherTransformed

logger = get_logger(__name__)


def kelvin_to_celsius(k: float) -> float:
    return round(k - 273.15, 2)


def ms_to_kmh(ms: float) -> float:
    return round(ms * 3.6, 2)


def get_humidity_category(humidity: int) -> str:
    if humidity < 30:
        return "Low"
    elif humidity < 70:
        return "Moderate"
    else:
        return "High"


class WeatherTransformer:

    def transform(self, raw_records: List[WeatherRaw]) -> List[WeatherTransformed]:
        """
        Transforms a list of raw weather records into cleaned,
        enriched records ready for the database.
        """
        logger.info(f"Transforming {len(raw_records)} records")
        results = []

        for raw in raw_records:
            try:
                temp_c = kelvin_to_celsius(raw.temperature_kelvin)
                feels_c = kelvin_to_celsius(raw.feels_like_kelvin)

                transformed = WeatherTransformed(
                    city=raw.city,
                    country=raw.country,
                    recorded_at=raw.recorded_at,
                    temperature_celsius=temp_c,
                    feels_like_celsius=feels_c,
                    temperature_delta=round(temp_c - feels_c, 2),
                    humidity_percent=raw.humidity_percent,
                    pressure_hpa=raw.pressure_hpa,
                    wind_speed_ms=raw.wind_speed_ms,
                    wind_speed_kmh=ms_to_kmh(raw.wind_speed_ms),
                    weather_condition=raw.weather_condition,
                    weather_description=raw.weather_description,
                    humidity_category=get_humidity_category(raw.humidity_percent)
                )
                results.append(transformed)
                logger.info(
                    f"{raw.city}: {temp_c}C, "
                    f"feels like {feels_c}C, "
                    f"{raw.weather_condition}, "
                    f"humidity {raw.humidity_percent}% ({transformed.humidity_category})"
                )

            except Exception as e:
                logger.error(f"Failed to transform record for {raw.city}: {e}")
                continue

        logger.info(f"Transformation complete: {len(results)} records ready")
        return results
