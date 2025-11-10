# validate_processed_data.py
"""
Policy 4900 Data Validation Script - PRODUCTION VERSION
=======================================================
Config-driven validation with normalization, real merge keys, date checks,
cross-file anomalies, and artifact generation.

Enhanced by Logan with:
- Data normalization pipeline
- Config-driven schema validation
- Real merge key logic matching Task2
- Filename vs column date validation
- Cross-file anomaly detection
- CSV artifact generation
- Validation marker creation

USAGE:
  python validate_processed_data.py                  # Validate all processed files
  python validate_processed_data.py --strict         # Treat warnings as errors
  python validate_processed_data.py --file FILE.csv  # Validate specific file
  python validate_processed_data.py --config PATH    # Use specific config file
  python validate_processed_data.py --no-cross-check # Skip cross-file analysis

EXIT CODES:
  0 = All checks passed
  1 = Critical errors found (blocks Task2)
  2 = Warnings found (review recommended but doesn't block Task2)

AUTHOR: Rob Zimmerman
ENHANCED: Logan (production features)
DATE: 2025-11-08
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass, field

import pandas as pd


# ============================================================================
# DATA NORMALIZATION HELPERS
# ============================================================================

def _clean_text(s: str) -> str:
    """Trim whitespace and collapse multiple spaces."""
    return re.sub(r"\s+", " ", str(s)).strip()


def _clean_pct(x) -> Optional[float]:
    """
    Clean percentage values - accepts "12", "12%", "12.0", " 12 % "
    Returns float or None for empty values.
    Raises ValueError for invalid formats.
    """
    s = str(x).strip().replace("%", "")
    if s == "" or s.lower() in ("nan", "none"):
        return None
    try:
        return float(s)
    except ValueError:
        raise ValueError(f"Invalid percentage: {x}")


def _clean_int(x) -> Optional[int]:
    """
    Clean integer values - accepts "123", "1,234", "12.0"
    Raises ValueError for non-integer strings.
    Returns None for empty values.
    """
    s = str(x).strip().replace(",", "")
    if s == "" or s.lower() in ("nan", "none"):
        return None
    if not re.fullmatch(r"-?\d+(\.0+)?", s):
        raise ValueError(f"Non-integer: {x}")
    return int(float(s))


def _normalize_room(room: str) -> str:
    """
    Normalize room numbers: strip spaces, uppercase, remove leading zeros.
    Examples: " 0102 " -> "102", "room 5" -> "ROOM5"
    """
    room_str = str(room).upper().replace(" ", "")
    # Remove leading zeros but keep if it's just "0" or "00"
    if room_str and room_str != "0":
        room_str = room_str.lstrip("0") or "0"
    return room_str


def _extract_date_from_filename(name: str) -> Optional[date]:
    """
    Extract date from filename like '11-06-2025_Policy4900_PROCESSED.csv'.
    Returns date object or None if not found.
    """
    m = re.search(r"(\d{2}-\d{2}-\d{4})", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%m-%d-%Y").date()
    except ValueError:
        return None


# ============================================================================
# VALIDATION RESULT CONTAINER
# ============================================================================

@dataclass
class ValidationResult:
    """Container for validation check results."""
    passed: bool
    severity: str  # 'ERROR' or 'WARNING'
    check_name: str
    message: str
    details: List[str] = field(default_factory=list)
    file_name: str = ""
    row_count: int = 0


# ============================================================================
# MAIN VALIDATOR CLASS
# ============================================================================

class ProcessedDataValidator:
    """
    Production-grade validator with:
    - Config-driven schema validation
    - Data normalization before checks
    - Real merge key duplicate detection
    - Date validation (filename vs column)
    - Cross-file anomaly detection
    - Artifact generation
    """

    def __init__(self, config_path: str = "config.json", strict_mode: bool = False):
        self.config_path = Path(config_path)
        self.config = self._load_config()
        self.strict_mode = strict_mode

        # Get paths from config
        paths = self.config.get("paths", {})
        self.new_reports_dir = Path(paths.get("new_reports_dir", ".")).expanduser().resolve()

        # Validation settings from config
        val_cfg = self.config.get("validation", {})
        self.strict_schema = val_cfg.get("strict_schema", True)
        self.require_validation = val_cfg.get("require_validation", True)
        self.swing_threshold = val_cfg.get("swing_threshold_pct", 50)
        self.max_students = val_cfg.get("max_students_per_room", 500)
        artifacts_dir = val_cfg.get("artifacts_dir", "Validation_Artifacts")
        self.artifacts_dir = Path(artifacts_dir).expanduser().resolve()

        # Column definitions from config
        cols_cfg = self.config.get("columns", {})
        self.required_fields = cols_cfg.get("required_fields", [
            "School of Instruction", "FISH Number", "Room", "Approval Status"
        ])
        self.numeric_fields = cols_cfg.get("numeric_fields", [
            "Total Student Count", "# of Students Opt In", "# of Students Opt Out",
            "# of Students No Response", "# of ESE Students"
        ])
        self.percentage_fields = cols_cfg.get("percentage_fields", [
            "% Opt In", "% Opt Out", "% No Response"
        ])
        self.valid_approval_statuses = cols_cfg.get("valid_approval_statuses", [
            "Approved", "Not Approved", "Withdrawn", "Void"
        ])
        self.expected_columns = cols_cfg.get("master_columns", [])

        # Tracking
        self.results: List[ValidationResult] = []
        self.processed_files_data: Dict[str, pd.DataFrame] = {}

        # Load Broward schools lookup
        self.broward_schools = self._load_broward_schools()

        logging.info(f"Validator initialized with config: {self.config_path}")
        logging.info(f"  Strict schema: {self.strict_schema}")
        logging.info(f"  Swing threshold: {self.swing_threshold}%")
        logging.info(f"  Max students: {self.max_students}")
        logging.info(f"  Artifacts dir: {self.artifacts_dir}")

    def _load_config(self) -> dict:
        """Load config file with defaults."""
        if not self.config_path.exists():
            logging.warning(f"Config file not found: {self.config_path}")
            return {"paths": {}, "columns": {}, "validation": {}}

        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Error loading config: {e}")
            return {"paths": {}, "columns": {}, "validation": {}}

    def _load_broward_schools(self) -> set:
        """Load valid school names from Broward Schools lookup file."""
        try:
            possible_paths = [
                Path("Broward_Schools_Mailto_Codes_and_Addresses.xlsx"),
                Path("/mnt/project/Broward_Schools_Mailto_Codes_and_Addresses.xlsx"),
                self.new_reports_dir.parent / "Broward_Schools_Mailto_Codes_and_Addresses.xlsx"
            ]

            for path in possible_paths:
                if path.exists():
                    logging.info(f"Loading Broward schools from: {path}")
                    df = pd.read_excel(path)
                    if "School" in df.columns:
                        schools = set(df["School"].dropna().astype(str))
                        logging.info(f"Loaded {len(schools)} valid school names")
                        return schools

            logging.warning("Broward schools lookup not found - school name validation disabled")
            return set()

        except Exception as e:
            logging.warning(f"Could not load Broward schools: {e}")
            return set()

    def find_processed_files(self, specific_file: str = None) -> List[Path]:
        """Find processed CSV files ready for validation."""
        if specific_file:
            path = Path(specific_file)
            if path.exists():
                return [path]
            else:
                logging.error(f"Specified file not found: {specific_file}")
                return []

        pattern = "*_Policy4900_PROCESSED.csv"
        processed_files = sorted(self.new_reports_dir.glob(pattern))
        logging.info(f"Found {len(processed_files)} processed file(s) to validate")
        return processed_files

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize all columns before validation:
        - Trim whitespace and collapse spaces in text fields
        - Clean and convert percentages
        - Clean and convert numeric fields
        - Normalize room numbers
        - Uppercase mail codes
        """
        df = df.copy()

        # Text fields - trim whitespace
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].map(_clean_text)

        # Percentages - clean and convert
        for col in self.percentage_fields:
            if col in df.columns:
                try:
                    df[col] = df[col].apply(_clean_pct)
                except ValueError as e:
                    logging.error(f"Error cleaning percentage column '{col}': {e}")
                    raise

        # Numeric fields - clean and convert with strict error checking
        for col in self.numeric_fields:
            if col in df.columns:
                try:
                    df[col] = df[col].apply(_clean_int)
                except ValueError as e:
                    logging.error(f"Error cleaning numeric column '{col}': {e}")
                    raise

        # Room normalization
        if "Room" in df.columns:
            df["Room"] = df["Room"].apply(_normalize_room)

        # Mail Code uppercase
        if "Mail Code" in df.columns:
            df["Mail Code"] = df["Mail Code"].str.upper()

        return df

    def _get_merge_key_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Determine which columns to use for merge key.
        Priority: School of Instruction | Mail Code | FISH Number | Room | School Code
        Uses whatever columns are available.
        """
        possible_cols = ["School of Instruction", "Mail Code", "FISH Number", "Room", "School Code"]
        key_cols = [c for c in possible_cols if c in df.columns]
        return key_cols

    def _create_merge_keys(self, df: pd.DataFrame) -> pd.Series:
        """Create merge keys using available columns."""
        key_cols = self._get_merge_key_columns(df)
        if not key_cols:
            logging.error("No merge key columns found in dataframe")
            return pd.Series([""] * len(df))

        # Create keys by joining available columns
        df_clean = df.copy()
        for col in key_cols:
            df_clean[col] = df_clean[col].astype(str)

        merge_keys = df_clean[key_cols].agg("|".join, axis=1)
        return merge_keys

    def validate_file(self, file_path: Path) -> None:
        """Run all validation checks on a single processed CSV file."""
        logging.info(f"\nValidating: {file_path.name}")
        logging.info("-" * 70)

        try:
            # Load raw data
            df_raw = pd.read_csv(file_path, dtype=str, keep_default_na=False)
            logging.info(f"  Loaded {len(df_raw)} rows, {len(df_raw.columns)} columns")

            # Basic checks on raw data
            self._check_empty_dataframe(df_raw, file_path.name)
            self._check_required_columns(df_raw, file_path.name)
            self._check_unexpected_columns(df_raw, file_path.name)

            # Normalize data
            try:
                df = self._normalize_columns(df_raw)
                logging.info("  Data normalization completed")
            except Exception as e:
                self.results.append(ValidationResult(
                    passed=False,
                    severity="ERROR",
                    check_name="Normalization Failed",
                    message=f"Could not normalize data: {e}",
                    file_name=file_path.name
                ))
                return

            # Store normalized data for cross-file checks
            self.processed_files_data[file_path.name] = df

            # Run all validation checks on normalized data
            self._check_missing_values(df, file_path.name)
            self._check_duplicate_records(df, file_path.name)
            self._check_numeric_ranges(df, file_path.name)
            self._check_logical_consistency(df, file_path.name)
            self._check_approval_status_values(df, file_path.name)
            self._check_percentage_validity(df, file_path.name)
            self._check_report_date(df, file_path.name)
            self._check_school_names(df, file_path.name)
            self._check_unusual_patterns(df, file_path.name)

            logging.info(f"  Validation complete for {file_path.name}")

        except Exception as e:
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="File Read Error",
                message=f"Could not process file: {e}",
                file_name=file_path.name
            ))

    def _check_empty_dataframe(self, df: pd.DataFrame, file_name: str) -> None:
        """Check if dataframe is empty."""
        if len(df) == 0:
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Empty File",
                message="Processed file contains no data rows",
                file_name=file_name
            ))

    def _check_required_columns(self, df: pd.DataFrame, file_name: str) -> None:
        """Verify all required columns are present."""
        missing = [col for col in self.required_fields if col not in df.columns]

        if missing:
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Missing Required Columns",
                message=f"File is missing {len(missing)} required column(s)",
                details=missing,
                file_name=file_name
            ))

    def _check_unexpected_columns(self, df: pd.DataFrame, file_name: str) -> None:
        """Check for unexpected columns if strict_schema is enabled."""
        if not self.strict_schema or not self.expected_columns:
            return

        unexpected = [col for col in df.columns if col not in self.expected_columns]

        if unexpected:
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Unexpected Columns",
                message=f"Found {len(unexpected)} unexpected column(s) in strict schema mode",
                details=unexpected,
                file_name=file_name
            ))

    def _check_missing_values(self, df: pd.DataFrame, file_name: str) -> None:
        """Check for None, empty strings in critical fields after normalization."""
        for col in self.required_fields:
            if col not in df.columns:
                continue

            # Check for various forms of missing data
            if df[col].dtype == object:
                mask = (df[col].isna() | (df[col] == "") | (df[col] == "nan"))
            else:
                mask = df[col].isna()

            missing_count = mask.sum()
            if missing_count > 0:
                missing_rows = df[mask].index.tolist()
                sample_rows = missing_rows[:5]

                self.results.append(ValidationResult(
                    passed=False,
                    severity="ERROR",
                    check_name=f"Missing Values - {col}",
                    message=f"Found {missing_count} row(s) with missing '{col}' values",
                    details=[f"Example rows (0-indexed): {sample_rows}"],
                    file_name=file_name,
                    row_count=missing_count
                ))

    def _check_duplicate_records(self, df: pd.DataFrame, file_name: str) -> None:
        """Check for duplicate records using real merge key logic."""
        # Create merge keys
        merge_keys = self._create_merge_keys(df)

        # Find duplicates
        duplicates_mask = merge_keys.duplicated(keep=False)

        if duplicates_mask.sum() > 0:
            dup_keys = merge_keys[duplicates_mask].unique().tolist()

            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Duplicate Records",
                message=f"Found {duplicates_mask.sum()} duplicate records using merge key",
                details=[f"Duplicates (first 10): {dup_keys[:10]}"],
                file_name=file_name,
                row_count=duplicates_mask.sum()
            ))

    def _check_numeric_ranges(self, df: pd.DataFrame, file_name: str) -> None:
        """Check for negative numbers and unrealistic values."""
        for col in self.numeric_fields:
            if col not in df.columns:
                continue

            # Check for negative values
            negative_mask = (df[col] < 0) & df[col].notna()
            negative_count = negative_mask.sum()

            if negative_count > 0:
                self.results.append(ValidationResult(
                    passed=False,
                    severity="ERROR",
                    check_name=f"Negative Values - {col}",
                    message=f"Found {negative_count} negative value(s) in '{col}'",
                    file_name=file_name,
                    row_count=negative_count
                ))

            # Check for unrealistically high values
            if col == "Total Student Count":
                high_mask = (df[col] > self.max_students) & df[col].notna()
                high_count = high_mask.sum()

                if high_count > 0:
                    high_schools = df[high_mask]["School of Instruction"].tolist()[:3]

                    self.results.append(ValidationResult(
                        passed=True,
                        severity="WARNING",
                        check_name="Unusually High Student Count",
                        message=f"Found {high_count} classroom(s) with >{self.max_students} students",
                        details=[f"Schools: {high_schools}"],
                        file_name=file_name,
                        row_count=high_count
                    ))

    def _check_logical_consistency(self, df: pd.DataFrame, file_name: str) -> None:
        """Verify mathematical relationships between fields."""
        required_cols = [
            "Total Student Count",
            "# of Students Opt In",
            "# of Students Opt Out",
            "# of Students No Response"
        ]

        if not all(col in df.columns for col in required_cols):
            return

        # All columns are now numeric from normalization
        total = df["Total Student Count"]
        opt_in = df["# of Students Opt In"]
        opt_out = df["# of Students Opt Out"]
        no_response = df["# of Students No Response"]

        # Check if Total = Opt In + Opt Out + No Response
        calculated_total = opt_in + opt_out + no_response
        mismatch_mask = (total != calculated_total) & total.notna() & calculated_total.notna()
        mismatch_count = mismatch_mask.sum()

        if mismatch_count > 0:
            examples = df[mismatch_mask][["School of Instruction", "Room"]].head(3)
            example_list = [f"{row['School of Instruction']}|{row['Room']}"
                            for _, row in examples.iterrows()]

            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Math Inconsistency",
                message=f"Found {mismatch_count} row(s) where Total ≠ OptIn + OptOut + NoResponse",
                details=[f"Examples: {example_list}"],
                file_name=file_name,
                row_count=mismatch_count
            ))

    def _check_approval_status_values(self, df: pd.DataFrame, file_name: str) -> None:
        """Verify approval status contains only valid values."""
        if "Approval Status" not in df.columns:
            return

        invalid_mask = ~df["Approval Status"].isin(self.valid_approval_statuses)
        invalid_count = invalid_mask.sum()

        if invalid_count > 0:
            invalid_values = df[invalid_mask]["Approval Status"].unique().tolist()

            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Invalid Approval Status",
                message=f"Found {invalid_count} row(s) with invalid Approval Status values",
                details=[f"Invalid values: {invalid_values}"],
                file_name=file_name,
                row_count=invalid_count
            ))

    def _check_percentage_validity(self, df: pd.DataFrame, file_name: str) -> None:
        """Check percentage fields are in valid range (0-100)."""
        for col in self.percentage_fields:
            if col not in df.columns:
                continue

            # Check for values outside 0-100 range
            invalid_mask = ((df[col] < 0) | (df[col] > 100)) & df[col].notna()
            invalid_count = invalid_mask.sum()

            if invalid_count > 0:
                invalid_values = df[col][invalid_mask].unique().tolist()[:5]

                self.results.append(ValidationResult(
                    passed=False,
                    severity="ERROR",
                    check_name=f"Invalid Percentage - {col}",
                    message=f"Found {invalid_count} percentage value(s) outside 0-100 range",
                    details=[f"Examples: {invalid_values}"],
                    file_name=file_name,
                    row_count=invalid_count
                ))

    def _check_report_date(self, df: pd.DataFrame, file_name: str) -> None:
        """Validate report date exists, is parseable, and matches filename."""
        if "Report Date" not in df.columns:
            # Report Date is optional for now, just warn
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Missing Report Date Column",
                message="Report Date column not found in file",
                file_name=file_name
            ))
            return

        # Try to parse first row's date
        try:
            col_date = pd.to_datetime(df["Report Date"].iloc[0]).date()
        except Exception:
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Invalid Report Date",
                message="Report Date column not parseable as date",
                file_name=file_name
            ))
            return

        # Check against filename date
        fn_date = _extract_date_from_filename(file_name)
        if fn_date and fn_date != col_date:
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Date Mismatch",
                message=f"Filename date {fn_date} != column date {col_date}",
                file_name=file_name
            ))

        # Check for future dates
        today = date.today()
        if col_date > today:
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Future Report Date",
                message=f"Report date {col_date} is in the future",
                file_name=file_name
            ))

        # Check for grossly old dates (>1 year ago)
        one_year_ago = today - timedelta(days=365)
        if col_date < one_year_ago:
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Old Report Date",
                message=f"Report date {col_date} is more than 1 year old",
                file_name=file_name
            ))

    def _check_school_names(self, df: pd.DataFrame, file_name: str) -> None:
        """Check if school names exist in Broward lookup database."""
        if not self.broward_schools or "School of Instruction" not in df.columns:
            return

        unknown_schools = set(df["School of Instruction"].unique()) - self.broward_schools
        unknown_schools = {s for s in unknown_schools if s.strip()}

        if unknown_schools:
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Unknown School Names",
                message=f"Found {len(unknown_schools)} school(s) not in Broward lookup",
                details=[f"Schools: {list(unknown_schools)[:10]}"],
                file_name=file_name
            ))

    def _check_unusual_patterns(self, df: pd.DataFrame, file_name: str) -> None:
        """Check for unusual patterns that may indicate data issues."""
        # High opt-out rate
        if "% Opt Out" in df.columns:
            high_opt_out = (df["% Opt Out"] > 80) & df["% Opt Out"].notna()
            high_count = high_opt_out.sum()

            if high_count > 0:
                schools = df[high_opt_out]["School of Instruction"].tolist()[:5]

                self.results.append(ValidationResult(
                    passed=True,
                    severity="WARNING",
                    check_name="High Opt-Out Rate",
                    message=f"Found {high_count} classroom(s) with >80% opt-out rate",
                    details=[f"Schools: {schools}"],
                    file_name=file_name,
                    row_count=high_count
                ))

        # Missing FISH List
        if "FISH List" in df.columns:
            missing_fish_list = (df["FISH List"] == "") | df["FISH List"].isna()
            missing_count = missing_fish_list.sum()

            if missing_count > 0:
                self.results.append(ValidationResult(
                    passed=True,
                    severity="WARNING",
                    check_name="Missing FISH List",
                    message=f"Found {missing_count} row(s) with missing FISH List values",
                    file_name=file_name,
                    row_count=missing_count
                ))

    def run_cross_file_checks(self) -> None:
        """
        Compare current processed files against previous day's data to detect anomalies:
        - Classrooms that vanished
        - Count swings > threshold
        - Approval status regressions
        """
        if len(self.processed_files_data) < 2:
            logging.info("Skipping cross-file checks (need at least 2 files)")
            return

        logging.info("\nRunning cross-file anomaly checks...")

        # Sort files by date
        sorted_files = sorted(self.processed_files_data.keys())

        # Compare each file to the previous one
        for i in range(1, len(sorted_files)):
            prev_file = sorted_files[i - 1]
            curr_file = sorted_files[i]

            prev_df = self.processed_files_data[prev_file]
            curr_df = self.processed_files_data[curr_file]

            logging.info(f"  Comparing {prev_file} → {curr_file}")
            self._check_vanished_classrooms(prev_df, curr_df, prev_file, curr_file)
            self._check_count_swings(prev_df, curr_df, prev_file, curr_file)
            self._check_approval_regressions(prev_df, curr_df, prev_file, curr_file)

    def _check_vanished_classrooms(self, prev_df: pd.DataFrame, curr_df: pd.DataFrame,
                                   prev_file: str, curr_file: str) -> None:
        """Check for classrooms that disappeared between reports."""
        prev_keys = set(self._create_merge_keys(prev_df))
        curr_keys = set(self._create_merge_keys(curr_df))

        vanished = prev_keys - curr_keys

        if vanished:
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Vanished Classrooms",
                message=f"{len(vanished)} classroom(s) present in {prev_file} but missing from {curr_file}",
                details=[f"Examples: {list(vanished)[:5]}"],
                file_name=curr_file
            ))

    def _check_count_swings(self, prev_df: pd.DataFrame, curr_df: pd.DataFrame,
                            prev_file: str, curr_file: str) -> None:
        """Check for large swings in student counts."""
        if "Total Student Count" not in prev_df.columns or "Total Student Count" not in curr_df.columns:
            return

        # Create merge keys for joining
        prev_df = prev_df.copy()
        curr_df = curr_df.copy()
        prev_df["_merge_key"] = self._create_merge_keys(prev_df)
        curr_df["_merge_key"] = self._create_merge_keys(curr_df)

        # Merge on key
        merged = prev_df[["_merge_key", "Total Student Count"]].merge(
            curr_df[["_merge_key", "Total Student Count"]],
            on="_merge_key",
            suffixes=("_prev", "_curr")
        )

        # Calculate percentage change (avoid division by zero)
        valid_prev = merged["Total Student Count_prev"] > 0
        merged_valid = merged[valid_prev].copy()

        if len(merged_valid) == 0:
            return

        merged_valid["pct_change"] = (
                (merged_valid["Total Student Count_curr"] - merged_valid["Total Student Count_prev"]) /
                merged_valid["Total Student Count_prev"] * 100
        ).abs()

        # Calculate absolute change in student count
        merged_valid["abs_change"] = (
                merged_valid["Total Student Count_curr"] -
                merged_valid["Total Student Count_prev"]
        ).abs()

        # Find large swings
        # Apply dual threshold: percent + minimum absolute change
        large_swings = merged_valid[
            (merged_valid["pct_change"] > self.swing_threshold) &
            (merged_valid["abs_change"] >= self.config["validation"].get("min_student_delta", 2))
            ]

        if len(large_swings) > 0:
            # Create readable examples with % and Δ count
            example_list = []
            for _, r in large_swings.head(5).iterrows():
                key = r["_merge_key"]
                delta = int(r["Total Student Count_curr"] - r["Total Student Count_prev"])
                sign = "↑" if delta > 0 else "↓"
                example_list.append(f"{key} (Δ={abs(delta)} {sign}, {round(r['pct_change'], 1)}%)")
                pct = round(r["pct_change"], 1)
                example_list.append(f"{key} (Δ={delta}, {pct}%)")

            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Large Count Swings",
                message=(
                    f"Found {len(large_swings)} classroom(s) with >{self.swing_threshold}% change "
                    f"in total student count (and ≥{self.config['validation'].get('min_student_delta', 2)} student delta)"
                ),
                details=[f"Examples: {example_list}"],
                file_name=curr_file
            ))

    def _check_approval_regressions(self, prev_df: pd.DataFrame, curr_df: pd.DataFrame,
                                    prev_file: str, curr_file: str) -> None:
        """Check for approval status regressions (Approved → Not Approved)."""
        if "Approval Status" not in prev_df.columns or "Approval Status" not in curr_df.columns:
            return

        prev_df = prev_df.copy()
        curr_df = curr_df.copy()
        prev_df["_merge_key"] = self._create_merge_keys(prev_df)
        curr_df["_merge_key"] = self._create_merge_keys(curr_df)

        # Merge on key
        merged = prev_df[["_merge_key", "Approval Status"]].merge(
            curr_df[["_merge_key", "Approval Status"]],
            on="_merge_key",
            suffixes=("_prev", "_curr")
        )

        # Find regressions
        regressions = merged[
            (merged["Approval Status_prev"] == "Approved") &
            (merged["Approval Status_curr"] == "Not Approved")
            ]

        if len(regressions) > 0:
            examples = regressions.head(5)["_merge_key"].tolist()

            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Approval Status Regressions",
                message=f"Found {len(regressions)} classroom(s) that regressed from Approved to Not Approved",
                details=[f"Examples: {examples}"],
                file_name=curr_file
            ))

    # ============================================================================
    # ARTIFACT GENERATION
    # ============================================================================

    def generate_artifacts(self) -> None:
        """Generate validation report CSV files."""
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Generate summary report
        report_data = []
        for result in self.results:
            report_data.append({
                "File": result.file_name,
                "Severity": result.severity,
                "Check": result.check_name,
                "Passed": result.passed,
                "Message": result.message,
                "Row Count": result.row_count,
                "Details": "; ".join(result.details) if result.details else ""
            })

        if report_data:
            report_df = pd.DataFrame(report_data)
            report_file = self.artifacts_dir / f"validation_report_{timestamp}.csv"
            report_df.to_csv(report_file, index=False)
            logging.info(f"Generated validation report: {report_file}")

            # Separate errors and warnings
            errors_df = report_df[report_df["Severity"] == "ERROR"]
            warnings_df = report_df[report_df["Severity"] == "WARNING"]

            if len(errors_df) > 0:
                errors_file = self.artifacts_dir / f"validation_errors_{timestamp}.csv"
                errors_df.to_csv(errors_file, index=False)
                logging.info(f"Generated errors file: {errors_file}")

            if len(warnings_df) > 0:
                warnings_file = self.artifacts_dir / f"validation_warnings_{timestamp}.csv"
                warnings_df.to_csv(warnings_file, index=False)
                logging.info(f"Generated warnings file: {warnings_file}")
        else:
            logging.info("No validation issues to report - no artifacts generated")

    def create_validation_markers(self, files: List[Path]) -> None:
        """Create .validated_ok marker files for files that passed validation."""
        errors = [r for r in self.results if r.severity == "ERROR"]

        if not errors:
            for file_path in files:
                marker = file_path.parent / f"{file_path.name}.validated_ok"
                marker.touch()
                logging.info(f"Created validation marker: {marker.name}")
        else:
            logging.warning("Errors found - validation markers NOT created")

    def print_summary(self) -> Tuple[int, int]:
        """Print validation summary and return (error_count, warning_count)."""
        errors = [r for r in self.results if r.severity == "ERROR"]
        warnings = [r for r in self.results if r.severity == "WARNING"]

        print("\n" + "=" * 70)
        print("VALIDATION SUMMARY")
        print("=" * 70)

        if not errors and not warnings:
            print("✓ ALL CHECKS PASSED")
            print("  No data quality issues detected.")
            return 0, 0

        # Print errors
        if errors:
            print(f"\n✗ CRITICAL ERRORS: {len(errors)}")
            print("-" * 70)
            for result in errors:
                print(f"\n[ERROR] {result.check_name}")
                print(f"  File: {result.file_name}")
                print(f"  {result.message}")
                if result.details:
                    for detail in result.details:
                        print(f"  • {detail}")

        # Print warnings
        if warnings:
            print(f"\n⚠ WARNINGS: {len(warnings)}")
            print("-" * 70)
            for result in warnings:
                print(f"\n[WARNING] {result.check_name}")
                print(f"  File: {result.file_name}")
                print(f"  {result.message}")
                if result.details:
                    for detail in result.details:
                        print(f"  • {detail}")

        print("\n" + "=" * 70)
        return len(errors), len(warnings)


# ============================================================================
# MAIN FUNCTION
# ============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Validate processed Policy 4900 CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--file",
        help="Validate specific file instead of all processed files"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Treat warnings as errors (exit code 1)"
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to config.json (default: config.json)"
    )
    parser.add_argument(
        "--no-cross-check",
        action="store_true",
        help="Skip cross-file anomaly detection"
    )

    args = parser.parse_args()

    # Setup logging
    log_dir = Path("C:\\BCPS\\ESE\\Logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    log_file = log_dir / f"validation_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

    print("=" * 70)
    print("Policy 4900 Data Validation - Production Version")
    print("=" * 70)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Config: {args.config}")
    print(f"Log File: {log_file}")
    print(f"Strict Mode: {args.strict}")

    # Initialize validator
    validator = ProcessedDataValidator(
        config_path=args.config,
        strict_mode=args.strict
    )

    # Find files to validate
    files = validator.find_processed_files(specific_file=args.file)

    if not files:
        print("\n[WARNING] No processed files found to validate")
        print("  Expected pattern: *_Policy4900_PROCESSED.csv")
        sys.exit(0)

    # Validate each file
    for file_path in files:
        validator.validate_file(file_path)

    # Run cross-file checks
    if not args.no_cross_check:
        validator.run_cross_file_checks()

    # Generate artifacts
    validator.generate_artifacts()

    # Create validation markers
    validator.create_validation_markers(files)

    # Print summary and determine exit code
    error_count, warning_count = validator.print_summary()

    print(f"\nEnd Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Artifacts: {validator.artifacts_dir}")
    print("=" * 70)

    # Exit codes
    if error_count > 0:
        print("\n[BLOCKED] Critical errors found - Task2 should not run")
        sys.exit(1)
    elif warning_count > 0 and args.strict:
        print("\n[BLOCKED] Warnings found in strict mode - Task2 blocked")
        sys.exit(1)
    elif warning_count > 0:
        print("\n[CAUTION] Warnings found - Review recommended but Task2 can proceed")
        sys.exit(2)
    else:
        print("\n[PASSED] All validation checks passed - Ready for Task2")
        sys.exit(0)


if __name__ == "__main__":
    main()