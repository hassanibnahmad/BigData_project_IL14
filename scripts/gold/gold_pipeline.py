
import logging

from scripts.common.minio_client import get_minio_client
from scripts.gold.gold_predictions_by_route import (
    read_silver_data,
    aggregate_predictions_by_route,
    write_gold_to_minio,
)

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# GOLD PIPELINE
# ============================================================

def run_gold_pipeline():
    """Execute the complete Silver-to-Gold pipeline."""

    logger.info("=" * 60)
    logger.info("Starting Gold Pipeline")
    logger.info("=" * 60)

    try:
        # Step 1: Connect to MinIO
        client = get_minio_client()
        logger.info("MinIO connection established.")

        # Step 2: Read Silver data
        silver_df = read_silver_data(client)

        if silver_df.empty:
            logger.warning("No Silver data available. Pipeline stopped.")
            return

        # Step 3: Transform Silver data into Gold
        gold_df = aggregate_predictions_by_route(silver_df)

        if gold_df.empty:
            logger.warning("Gold DataFrame is empty. Pipeline stopped.")
            return

        # Step 4: Write Gold data to MinIO
        gold_file = write_gold_to_minio(client, gold_df)

        # Step 5: Display execution summary
        logger.info("=" * 60)
        logger.info("Gold Pipeline completed successfully.")
        logger.info("Routes processed: %s", len(gold_df))
        logger.info("Gold file: %s", gold_file)
        logger.info("=" * 60)

        return gold_df

    except Exception:
        logger.exception("Gold Pipeline failed.")
        raise


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_gold_pipeline()
