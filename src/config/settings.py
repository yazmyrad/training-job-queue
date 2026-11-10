import os


class Settings:
    database_url: str = os.environ.get(
        "DATABASE_URL", "postgresql://soup:soup@localhost:5432/soup"
    )


settings = Settings()