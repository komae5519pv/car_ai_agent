from pathlib import Path

app_name = "car-ai-demo"
app_entrypoint = "car_ai_demo.backend.app:app"
app_slug = "car_ai_demo"
api_prefix = "/api"
dist_dir = Path(__file__).parent / "__dist__"
