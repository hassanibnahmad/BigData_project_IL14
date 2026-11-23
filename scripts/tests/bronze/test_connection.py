from minio_client import get_minio_client, bucket_exists
from config import MINIO_BUCKET


def test_minio_connection():
    print("Testing connection to MinIO...")

    client = get_minio_client()

    # Real API request to MinIO
    response = client.list_buckets()

    print("\nConnection successful!")
    print("Available buckets:")

    for bucket in response["Buckets"]:
        print(f"- {bucket['Name']}")

    # Verify target bucket
    if bucket_exists(client, MINIO_BUCKET):
        print(f"\nBucket '{MINIO_BUCKET}' exists.")
    else:
        print(f"\nBucket '{MINIO_BUCKET}' NOT FOUND.")


if __name__ == "__main__":
    test_minio_connection()