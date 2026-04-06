"""
Extractor for the Weather ETL Pipeline.

Responsibilities:
- Call the OpenWeatherMap API for each city
- Handle errors, retries, and rate limits gracefully
- Return raw validated data — no transformations here
- Separation of concerns: extractor ONLY extracts
"""

import time
import requests
from datetime import datetime
from typing import List, Optional

from src.logger import get_logger
from src.models import WeatherRaw

logger = get_logger(__name__)

BASE_URL = "http://api.openweathermap.org/data/2.5/weather"


class WeatherExtractor:

    def __init__(self, api_key: str, max_retries: int = 3):
        self.api_key = api_key
        self.max_retries = max_retries
        logger.info(f"WeatherExtractor ready — max retries: {max_retries}")

    def _fetch_city(self, city: str) -> Optional[dict]:
        """
        Fetches raw JSON for one city with retry logic.
        Exponential backoff: wait 1s, 2s, 4s between retries.
        This is production-grade resilience.
        """
        params = {
            "q": city,
            "appid": self.api_key,
            "units": "standard"  # Kelvin — we convert in transformer
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Fetching weather for {city} (attempt {attempt})")
                response = requests.get(
                    BASE_URL,
                    params=params,
                    timeout=10
                )

                if response.status_code == 200:
                    logger.info(f"Successfully fetched data for {city}")
                    return response.json()

                elif response.status_code == 401:
                    logger.error("Invalid API key — check your OPENWEATHER_API_KEY")
                    return None

                elif response.status_code == 404:
                    logger.error(f"City not found: {city}")
                    return None

                elif response.status_code == 429:
                    wait = 60
                    logger.warning(f"Rate limited — waiting {wait}s")
                    time.sleep(wait)

                else:
                    logger.warning(
                        f"Unexpected status {response.status_code} "
                        f"for {city} on attempt {attempt}"
                    )

            except requests.exceptions.Timeout:
                logger.warning(f"Timeout for {city} on attempt {attempt}")

            except requests.exceptions.ConnectionError:
                logger.warning(f"Connection error for {city} on attempt {attempt}")

            except Exception as e:
                logger.error(f"Unexpected error for {city}: {e}")
                return None

            # Exponential backoff before retry
            if attempt < self.max_retries:
                wait = 2 ** (attempt - 1)
                logger.info(f"Retrying in {wait}s...")
                time.sleep(wait)

        logger.error(f"All {self.max_retries} attempts failed for {city}")
        return None

    def _parse_response(self, raw: dict, city: str) -> Optional[WeatherRaw]:
        """
        Parses raw API JSON into a validated WeatherRaw Pydantic model.
        If parsing fails, logs the error and returns None.
        Bad data never reaches the transformer.
        """
        try:
            return WeatherRaw(
                city=raw["name"],
                country=raw["sys"]["country"],
                recorded_at=datetime.utcfromtimestamp(raw["dt"]),
                temperature_kelvin=raw["main"]["temp"],
                feels_like_kelvin=raw["main"]["feels_like"],
                humidity_percent=raw["main"]["humidity"],
                pressure_hpa=raw["main"]["pressure"],
                wind_speed_ms=raw["wind"]["speed"],
                weather_condition=raw["weather"][0]["main"],
                weather_description=raw["weather"][0]["description"]
            )
        except KeyError as e:
            logger.error(f"Missing field in API response for {city}: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to parse response for {city}: {e}")
            return None

    def extract(self, cities: List[str]) -> List[WeatherRaw]:
        """
        Main extract method — fetches and validates all cities.
        Returns only successfully parsed records.
        Failed cities are logged but don't stop the pipeline.
        """
        logger.info(f"Starting extraction for {len(cities)} cities: {cities}")
        results = []

        for city in cities:
            raw_json = self._fetch_city(city)
            if raw_json is None:
                continue

            parsed = self._parse_response(raw_json, city)
            if parsed is not None:
                results.append(parsed)

        logger.info(
            f"Extraction complete: {len(results)}/{len(cities)} cities successful"
        )
        return results
