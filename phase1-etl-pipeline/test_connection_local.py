from src.loader import WeatherLoader

DB_URL = "postgresql://postgres:postgres@127.0.0.1:5432/weather_pipeline"

loader = WeatherLoader(DB_URL)
loader.create_tables()
run_id = loader.start_pipeline_run()
print("Run ID:", run_id)
loader.finish_pipeline_run(
    run_id,
    status="success",
    rows_extracted=5,
    rows_loaded=5,
    duration_seconds=1.2
)
print("Pipeline run tracked successfully!")
