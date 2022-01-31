
import os


def get_postgres_uri():
    host = os.environ.get("DB_HOST", "localhost")
    port = 30000 if host == "localhost" else 5432
    password = os.environ.get("DB_PASSWORD", "example")
    user, db_name = "cosmic", "cosmic_db"
    return f"postgresql://{user}:{password}@{host}:{port}/{db_name}"


def get_api_url():
    host = os.environ.get("API_HOST", "localhost")
    port = 5005 if host == "localhost" else 80
    return f"http://{host}:{port}"
