# task1_process_reports.py

"""
TASK 1: Process New Daily Reports
==================================
Reads raw Policy 4900 CSV reports and prepares them for Step 2 processing.

Input:  *_Policy4900.csv files (without .step1 marker)
Output: *_Policy4900_PROCESSED.csv files + .step1 marker files

This script does NOT touch the master tracking file.
The processed CSV files will be consumed by the Step 2 script.
"""

import logging
import sys
import json
from pathlib import Path
from datetime import datetime

import pandas as pd


def setup_logging(script_name: str, config: dict | None = None):
    from datetime import datetime
    from pathlib import Path
    log_dir_str = (config or {}).get("paths", {}).get("log_dir", "Logs")
    log_dir = Path(log_dir_str).expanduser().resolve()
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_file = log_dir / f"{script_name}_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler()],
        force=True,
    )
    logging.info("=" * 70)
    logging.info(f"Log file: {log_file}")
    logging.info("=" * 70)
    return log_file



class ReportProcessor:
    """Process raw Policy 4900 reports into standardized format."""

    def __init__(self, config_path: str = "config.json"):
        self.config_path = Path(config_path)
        self.config = self._load_config()

        # Get paths from config
        io = self.config.get("io", {})
        paths = self.config.get("paths", {})

        new_reports_dir = io.get("new_reports_dir") or paths.get("new_reports_dir") or "."
        self.new_reports_dir = Path(new_reports_dir).expanduser().resolve()

        logging.info(f"New reports directory: {self.new_reports_dir}")

        # Column definitions
        columns_cfg = self.config.get("columns", {})

        # Expected columns from source reports
        self.report_columns = columns_cfg.get("report_columns") or [
            "School of Instruction",
            "FISH Number",
            "Room",
            "FISH List",
            "Total Student Count",
            "# of Students Opt In",
            "# of Students Opt Out",
            "# of Students No Response",
            "% Opt In",
            "% Opt Out",
            "% No Response",
            "# of ESE Students",
            "Mark if Void",
        ]

        # All columns for processed output
        self.master_columns = columns_cfg.get("master_columns") or [
            "School of Instruction",
            "FISH Number",
            "Room",
            "FISH List",
            "Total Student Count",
            "# of Students Opt In",
            "# of Students Opt Out",
            "# of Students No Response",
            "% Opt In",
            "% Opt Out",
            "% No Response",
            "# of ESE Students",
            "Mark if Void",
            "Date First Seen",
            "Change Control",
            "Previous Approval Status",
            "Approval Status",
            "Date Added to Installation List",
            "Installation Status",
            "Installation Date",
            "Activation Status",
            "Activation Date",
            "Camera Source School",
            "Camera Source Classroom",
            "Camera Type",
            "Notes",
        ]

        self.date_format = self.config.get("output", {}).get("date_format", "%m-%d-%Y")

    def _load_config(self) -> dict:
        """Load config file with defaults."""
        default = {
            "io": {},
            "paths": {},
            "columns": {},
            "output": {"date_format": "%m-%d-%Y"},
        }
        if not self.config_path.exists():
            logging.warning(f"Config file not found: {self.config_path}. Using defaults.")
            return default

        with open(self.config_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        for k, v in default.items():
            data.setdefault(k, v)
        return data

    def get_unprocessed_reports(self):
        """Find CSV reports that haven't been processed (no .step1 marker)."""
        pattern = "*_Policy4900.csv"
        logging.info(f"Scanning for: {pattern}")

        if not self.new_reports_dir.exists():
            logging.error(f"Directory does not exist: {self.new_reports_dir}")
            return []

        all_csvs = sorted(self.new_reports_dir.glob(pattern))
        logging.info(f"Found {len(all_csvs)} CSV file(s)")

        unprocessed = []
        for csv_path in all_csvs:
            step1_marker = csv_path.parent / f"{csv_path.name}.step1"
            if step1_marker.exists():
                logging.info(f"  âœ“ Already processed (step1): {csv_path.name}")
            else:
                logging.info(f"  â†’ Ready for processing: {csv_path.name}")
                unprocessed.append(csv_path)

        logging.info(f"Unprocessed reports: {len(unprocessed)}")
        return unprocessed

    def read_csv_robust(self, path: Path) -> pd.DataFrame:
        """Read CSV with multiple encoding attempts."""
        encodings = ["utf-8", "utf-8-sig", "windows-1252", "latin-1"]

        for encoding in encodings:
            try:
                df = pd.read_csv(path, dtype=str, encoding=encoding)
                logging.info(f"  Successfully read with {encoding} encoding")
                return df
            except UnicodeDecodeError:
                continue

        raise ValueError(f"Could not read CSV with any supported encoding: {path}")

    def process_report(self, report_path: Path) -> pd.DataFrame:
        """
        Process a raw report into standardized format.

        Steps:
        1. Read CSV
        2. Normalize columns
        3. Type-cast data
        4. Calculate Approval Status
        5. Apply Void logic
        6. Add all master columns
        """
        logging.info(f"\nProcessing: {report_path.name}")

        # Read the file
        df = self.read_csv_robust(report_path)

        # Normalize headers
        df.columns = df.columns.str.strip()
        df = df.loc[:, ~df.columns.str.contains(r"^Unnamed", na=False)]

        # Keep only expected columns
        present = [c for c in self.report_columns if c in df.columns]
        if not present:
            raise ValueError(f"No expected columns found in {report_path.name}")

        df = df[present].copy()

        # Add missing report columns as empty
        for col in self.report_columns:
            if col not in df.columns:
                df[col] = ""

        logging.info(f"  Columns present: {len(present)}/{len(self.report_columns)}")

        # Cast identifier columns to string
        for col in ["School of Instruction", "FISH Number", "Room", "FISH List"]:
            df[col] = df[col].astype(str).fillna("")

        # Cast numeric count columns
        for col in [
            "Total Student Count",
            "# of Students Opt In",
            "# of Students Opt Out",
            "# of Students No Response",
            "# of ESE Students",
        ]:
            s = df[col].astype(str).str.replace(",", "", regex=False).str.strip()
            df[col] = pd.to_numeric(s, errors="coerce").fillna(0).astype(int)

        # Parse percentage columns from CSV data (preserve source percentages)
        # The CSV already has calculated percentages like "100.00%", "0.68%", etc.
        for col in ["% Opt In", "% Opt Out", "% No Response"]:
            if col in df.columns:
                # Remove % sign and convert to numeric, then round to whole number
                s = df[col].astype(str).str.replace("%", "", regex=False).str.strip()
                pct = pd.to_numeric(s, errors="coerce")
                # Round to whole numbers, store as nullable Int64 so NaN stays blank in CSV
                df[col] = pct.round(0).astype("Int64")

        # Ensure Mark if Void exists
        if "Mark if Void" not in df.columns:
            df["Mark if Void"] = ""

        # Calculate Approval Status
        df["Approval Status"] = df.apply(self.calculate_approval_status, axis=1)
        logging.info(f"  Calculated Approval Status")

        # Apply Void logic to tracking columns
        void_mask = df["Approval Status"] == "Void"
        void_columns = [
            "Date First Seen",
            "Date Added to Installation List",
            "Installation Status",
            "Installation Date",
            "Activation Status",
            "Activation Date",
            "Camera Source School",
            "Camera Source Classroom",
            "Camera Type"
        ]

        for col in void_columns:
            if col not in df.columns:
                df[col] = ""
            df.loc[void_mask, col] = "Void"

        # Add all master columns that aren't present yet
        for col in self.master_columns:
            if col not in df.columns:
                df[col] = ""

        # NEW CODE - Populate Report Date from filename
        report_date = self.extract_date_from_filename(report_path.name)
        df["Report Date"] = report_date.strftime(self.date_format)
        logging.info(f"  Set Report Date: {df['Report Date'].iloc[0]}")

        # Reorder columns to match master column order
        final_cols = [c for c in self.master_columns if c in df.columns]
        df = df[final_cols].copy()

        logging.info(f"  Output columns: {len(final_cols)}")
        logging.info(f"  Total rows: {len(df)}")

        # Count approval statuses
        status_counts = df["Approval Status"].value_counts()
        for status, count in status_counts.items():
            logging.info(f"    {status}: {count}")

        return df

    @staticmethod
    def calculate_approval_status(row) -> str:
        """
        Calculate approval status based on student counts and void flag.

        Priority:
        1. Check Mark if Void
        2. Check if any students
        3. Check for opt-outs
        4. Check for 100% opt-in
        5. Check for no responses
        """
        # FIRST: Check for void flag
        mark_if_void = str(row.get("Mark if Void", "") or "").strip()
        if mark_if_void.lower() in {"y", "yes", "void", "true", "1"} or "void" in mark_if_void.lower():
            return "Void"

        # SECOND: Check student counts
        try:
            total = int(row.get("Total Student Count", 0) or 0)
            opt_in = int(row.get("# of Students Opt In", 0) or 0)
            opt_out = int(row.get("# of Students Opt Out", 0) or 0)
            no_resp = int(row.get("# of Students No Response", 0) or 0)
        except Exception:
            total = opt_in = opt_out = no_resp = 0

        if total == 0:
            return "Not Requested"
        if opt_out > 0:
            return "Denied - Parent Opt Out"
        if opt_in == total and total > 0:
            return "Approved - Camera Authorized"
        if no_resp > 0:
            return "Awaiting Responses"

        return "Not Requested"

    def write_processed_report(self, df: pd.DataFrame, original_path: Path):
        """Write processed dataframe to CSV file."""
        # Create output filename
        stem = original_path.stem  # e.g., "10-15-2025_Policy4900"
        output_name = f"{stem}_PROCESSED.csv"
        output_path = original_path.parent / output_name

        logging.info(f"  Writing: {output_name}")

        # Write to CSV (UTF-8 encoding, no index)
        df.to_csv(output_path, index=False, encoding="utf-8")

        logging.info(f"  âœ“ Created: {output_path}")
        return output_path

    def create_step1_marker(self, original_path: Path, processed_path: Path, df: pd.DataFrame):
        """Create .step1 marker file with processing metadata."""
        marker_path = original_path.parent / f"{original_path.name}.step1"

        metadata = {
            "source_file": original_path.name,
            "processed_file": processed_path.name,
            "processed_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "row_count": len(df),
            "report_date": self.extract_date_from_filename(original_path.name).strftime(self.date_format),
            "status_summary": df["Approval Status"].value_counts().to_dict()
        }

        with open(marker_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        logging.info(f"  âœ“ Created marker: {marker_path.name}")
        return marker_path

    @staticmethod
    def extract_date_from_filename(name: str) -> datetime:
        """Extract date from filename like '10-15-2025_Policy4900.csv'."""
        stem = Path(name).stem
        date_str = stem[:10]
        return datetime.strptime(date_str, "%m-%d-%Y")


def main():
    processor = ReportProcessor()  # builds self.config
    log_file = setup_logging("task1_process_reports", processor.config)

    logging.info("=" * 70)
    logging.info("   TASK 1: Process New Policy 4900 Reports")
    logging.info("=" * 70)
    logging.info("")

    logging.info(f"Reports directory: {processor.new_reports_dir}")
    logging.info("")

    # Find unprocessed reports
    reports = processor.get_unprocessed_reports()

    if not reports:
        logging.info("\nâœ“ No new reports to process")
        logging.info("\n" + "=" * 70)
        logging.info("   Task 1 Complete - Nothing to Process")
        logging.info("=" * 70)
        return

    logging.info(f"\nFound {len(reports)} report(s) to process\n")

    # Process each report
    processed_count = 0
    failed_count = 0

    for report_path in reports:
        try:
            # Process the report
            df_processed = processor.process_report(report_path)

            # Write output file
            output_path = processor.write_processed_report(df_processed, report_path)

            # Create step1 marker
            processor.create_step1_marker(report_path, output_path, df_processed)

            processed_count += 1
            logging.info("")

        except Exception as e:
            logging.error(f"Failed to process {report_path.name}: {e}")
            failed_count += 1
            logging.info("")

    # Summary
    logging.info("=" * 70)
    logging.info("   Task 1 Summary")
    logging.info("=" * 70)
    logging.info(f"Successfully processed: {processed_count}")
    logging.info(f"Failed: {failed_count}")
    logging.info("")

    if processed_count > 0:
        logging.info("âœ“ Review the *_PROCESSED.csv files before running Task 2")

    logging.info("=" * 70)


if __name__ == "__main__":
    try:
        main()
    except Exception as ex:
        logging.exception(ex)
        sys.exit(1)