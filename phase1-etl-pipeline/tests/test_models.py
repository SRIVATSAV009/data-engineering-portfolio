"""
Basic smoke tests for Pydantic models.
"""
from datetime import datetime
from src.models import WeatherRaw, WeatherTransformed, PipelineRun


def test_weather_raw_valid():
    raw = WeatherRaw(
        city="dallas",
        country="us",
        recorded_at=datetime.now(),
        temperature_kelvin=300.15,
        feels_like_kelvin=298.15,
        humidity_percent=65,
        pressure_hpa=1013.25,
        wind_speed_ms=5.5,
        weather_condition="Clear",
        weather_description="clear sky"
    )
    assert raw.city == "Dallas"
    assert raw.country == "US"


def test_weather_raw_rejects_bad_humidity():
    import pytest
    with pytest.raises(Exception):
        WeatherRaw(
            city="dallas",
            country="us",
            recorded_at=datetime.now(),
            temperature_kelvin=300.15,
            feels_like_kelvin=298.15,
            humidity_percent=150,
            pressure_hpa=1013.25,
            wind_speed_ms=5.5,
            weather_condition="Clear",
            weather_description="clear sky"
        )


def test_pipeline_run_valid():
    run = PipelineRun(
        run_id="test-123",
        started_at=datetime.now(),
        status="running"
    )
    assert run.status == "running"
