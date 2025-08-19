import os
import shutil
from pathlib import Path
from datetime import datetime

# Define paths
BASE_DIR = Path(__file__).resolve().parents[1]
incoming = BASE_DIR / "data" / "incoming"
archive = BASE_DIR / "data" / "archive"

def extract_latest_csv() -> str:
    """
    Picks the most recent CSV from incoming/ and copies it to archive/.
    Returns full path to the extracted file.
    """
    incoming.mkdir(parents=True, exist_ok=True)
    archive.mkdir(parents=True, exist_ok=True)

    # Get the latest CSV based on modified time
    csvs = sorted(incoming.glob("*.csv"), key=lambda f: f.stat().st_mtime, reverse=True)

    if not csvs:
        raise FileNotFoundError("No CSVs found in data/incoming/")

    latest_csv = csvs[0]
    print(f"[Extract] Latest file found: {latest_csv.name}")

    # Copy to archive with timestamp prefix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archived_name = f"{timestamp}__{latest_csv.name}"
    archived_path = archive / archived_name

    shutil.copy2(latest_csv, archived_path)
    print(f"[Extract] Archived as: {archived_path.name}")

    return str(latest_csv.resolve())  # Pass to transform via XCom
