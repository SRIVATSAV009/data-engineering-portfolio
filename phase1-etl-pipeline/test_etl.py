import os
from dotenv import load_dotenv
from src.extractor import WeatherExtractor
from src.transformer import WeatherTransformer

load_dotenv()

API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITIES = os.getenv("CITIES", "London,Dallas,Tokyo").split(",")

# EXTRACT
extractor = WeatherExtractor(api_key=API_KEY)
raw_records = extractor.extract(CITIES)
print(f"\nExtracted: {len(raw_records)} records")

# TRANSFORM
transformer = WeatherTransformer()
transformed = transformer.transform(raw_records)
print(f"Transformed: {len(transformed)} records")

# Preview
for rec in transformed:
    print(f"\n{rec.city}, {rec.country}")
    print(f"  Temp: {rec.temperature_celsius}C (feels {rec.feels_like_celsius}C)")
    print(f"  Humidity: {rec.humidity_percent}% — {rec.humidity_category}")
    print(f"  Wind: {rec.wind_speed_kmh} km/h")
    print(f"  Condition: {rec.weather_description}")
