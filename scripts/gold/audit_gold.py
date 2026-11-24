
import logging

from scripts.common.minio_client import get_minio_client
from scripts.gold.gold_predictions_by_route import read_silver_data
from scripts.gold.validate_gold import get_latest_gold_file, read_gold_file

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)


def audit_gold():
    logger.info("Starting Silver vs Gold audit...")

    client = get_minio_client()

    # Load Silver data
    silver_df = read_silver_data(client)

    if silver_df.empty:
        logger.error("Silver dataset is empty.")
        return

    # Load latest Gold data
    gold_key = get_latest_gold_file(client)

    if gold_key is None:
        logger.error("No Gold file found.")
        return

    gold_df = read_gold_file(client, gold_key)

    # Recalculate metrics from Silver
    expected_predictions = silver_df["prediction_id"].count()
    expected_trips = silver_df["trip_id"].nunique()
    expected_stops = silver_df["stop_id"].nunique()
    expected_vehicles = silver_df["vehicle_id"].nunique()

    # Extract Gold metrics
    gold_row = gold_df.iloc[0]

    print("\n" + "=" * 60)
    print("SILVER VS GOLD AUDIT")
    print("=" * 60)

    audit_results = {
        "total_predictions": (expected_predictions, gold_row["total_predictions"]),
        "total_trips": (expected_trips, gold_row["total_trips"]),
        "total_stops": (expected_stops, gold_row["total_stops"]),
        "total_vehicles": (expected_vehicles, gold_row["total_vehicles"]),
    }

    for metric, (expected, actual) in audit_results.items():
        status = "PASS" if expected == actual else "FAIL"

        print(
            f"{metric:<25} | "
            f"Silver: {expected:<5} | "
            f"Gold: {actual:<5} | "
            f"{status}"
        )

    print("=" * 60)

    if all(expected == actual for expected, actual in audit_results.values()):
        logger.info("Audit successful: Silver and Gold metrics are consistent.")
    else:
        logger.warning("Audit failed: discrepancies detected.")


if __name__ == "__main__":
    audit_gold()
