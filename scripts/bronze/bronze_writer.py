import json
import sys
from pathlib import Path
from datetime import datetime, timezone
from io import BytesIO

# Locate the project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Shared modules
COMMON_PATH = PROJECT_ROOT / "scripts" / "common"
sys.path.append(str(COMMON_PATH))

from minio_client import get_minio_client
from config import MINIO_BUCKET


class BronzeWriter:

    def __init__(self):
        self.client = get_minio_client()
        self.bucket = MINIO_BUCKET

    def write_batch(self, topic, messages):
        if not messages:
            print("No messages to write.")
            return None

        now = datetime.now(timezone.utc)

        # Generate JSON Lines content
        lines = []

        for message in messages:
            record = {
                "kafka_metadata": {
                    "topic": message["topic"],
                    "partition": message["partition"],
                    "offset": message["offset"],
                    "timestamp": message["timestamp"],
                },
                "data": message["value"],
            }

            lines.append(json.dumps(record, ensure_ascii=False))

        content = "\n".join(lines) + "\n"
        data = content.encode("utf-8")

        # Create a unique file name
        file_name = (
            f"bronze/{topic}/"
            f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
            f"batch_{now.strftime('%Y%m%d_%H%M%S_%f')}.jsonl"
        )

        # Upload to MinIO
        self.client.put_object(
            Bucket=self.bucket,
            Key=file_name,
            Body=BytesIO(data),
            ContentLength=len(data),
            ContentType="application/json",
        )
        

        print(f"Batch successfully stored in MinIO: {file_name}")
        print(f"Messages written: {len(messages)}")

        return file_name