import os
from pathlib import Path
from dotenv import load_dotenv

# Determine the project root directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Load the .env file located at the project root
ENV_FILE = PROJECT_ROOT / ".env"
load_dotenv(ENV_FILE)

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")


def validate_config():
    """Validate that all required MinIO configuration variables are defined."""

    required_variables = {
        "MINIO_ENDPOINT": MINIO_ENDPOINT,
        "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
        "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
        "MINIO_BUCKET": MINIO_BUCKET,
    }

    missing_variables = [
        name for name, value in required_variables.items()
        if not value
    ]

    if missing_variables:
        raise ValueError(
            f"Missing configuration variables: {', '.join(missing_variables)}"
        )


if __name__ == "__main__":
    validate_config()
    print("MinIO configuration loaded successfully!")
    print(f"Endpoint: {MINIO_ENDPOINT}")
    print(f"Bucket: {MINIO_BUCKET}")
    print("Credentials: loaded (hidden)")