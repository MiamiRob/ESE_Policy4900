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
        self.swing_threshold = val_cfg.get("swing_threshold_pct", 50)
        self.min_student_delta = val_cfg.get("min_student_delta", 2)
        self.max_students = val_cfg.get("max_students_per_room", 500)
        self.artifacts_dir = Path(val_cfg.get("artifacts_dir", "Validation_Artifacts")).expanduser().resolve()

        # Column definitions from config
        columns_cfg = self.config.get("columns", {})
        self.expected_columns = columns_cfg.get("master_columns", [])
        self.required_fields = columns_cfg.get("required_fields", [
            "School of Instruction", "FISH Number", "Room", "Approval Status"
        ])
        self.numeric_fields = columns_cfg.get("numeric_fields", [
            "Total Student Count", "# of Students Opt In", "# of Students Opt Out",
            "# of Students No Response", "# of ESE Students"
        ])
        self.percentage_fields = columns_cfg.get("percentage_fields", [
            "% Opt In", "% Opt Out", "% No Response"
        ])
        self.valid_statuses = columns_cfg.get("valid_approval_statuses", [
            "Approved - Camera Authorized", "Denied - Parent Opt Out",
            "Awaiting Responses", "Void", "Not Requested", "Ineligible - FISH List N"
        ])

        # Results storage
        self.results: List[ValidationResult] = []
        self.processed_files_data: Dict[str, pd.DataFrame] = {}

        logging.info(f"Validator initialized with config: {config_path}")
        logging.info(f"Reports directory: {self.new_reports_dir}")
        logging.info(f"Artifacts directory: {self.artifacts_dir}")

    def _load_config(self) -> dict:
        """Load config file with sensible defaults."""
        if not self.config_path.exists():
            logging.warning(f"Config not found: {self.config_path}. Using defaults.")
            return {"paths": {}, "columns": {}, "validation": {}}
        with open(self.config_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def find_processed_files(self, specific_file: Optional[str] = None) -> List[Path]:
        """Find processed CSV files ready for validation."""
        if specific_file:
            path = Path(specific_file)
            if not path.exists():
                path = self.new_reports_dir / specific_file
            if path.exists():
                return [path]
            logging.error(f"File not found: {specific_file}")
            return []

        pattern = "*_Policy4900_PROCESSED.csv"
        files = sorted(self.new_reports_dir.glob(pattern))
        logging.info(f"Found {len(files)} processed file(s)")
        return files

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply normalization to all columns."""
        df = df.copy()

        # Text columns
        text_cols = ["School of Instruction", "FISH Number", "FISH List", "Mark if Void",
                     "Approval Status", "Parent Consent Status", "Installation Status",
                     "Activation Status", "Change Control"]
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: _clean_text(str(x)) if pd.notna(x) else "")

        # Room normalization
        if "Room" in df.columns:
            df["Room"] = df["Room"].apply(lambda x: _normalize_room(str(x)) if pd.notna(x) else "")

        # Numeric columns
        for col in self.numeric_fields:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: _clean_int(x) if pd.notna(x) and str(x).strip() else None)

        # Percentage columns
        for col in self.percentage_fields:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: _clean_pct(x) if pd.notna(x) and str(x).strip() else None)

        return df

    def _create_merge_keys(self, df: pd.DataFrame) -> pd.Series:
        """Create merge keys exactly matching Task2's key_sf logic."""
        df_clean = df.copy()

        for col in ["School of Instruction", "Room", "FISH Number"]:
            if col not in df_clean.columns:
                df_clean[col] = ""
            df_clean[col] = df_clean[col].astype(str).str.strip().str.lower()

        school = df_clean["School of Instruction"]
        room = df_clean["Room"]
        fish = df_clean["FISH Number"]

        key = school + "|" + room
        return key.where(room.ne(""), school + "|" + fish)

    def validate_file(self, file_path: Path) -> None:
        """Run all validation checks on a single processed CSV file."""
        logging.info(f"\nValidating: {file_path.name}")
        logging.info("-" * 70)

        try:
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

            # Store for cross-file checks
            self.processed_files_data[file_path.name] = df

            # Run validation checks
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

    # ========================================================================
    # INDIVIDUAL VALIDATION CHECKS
    # ========================================================================

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
                passed=True,
                severity="WARNING",
                check_name="Unexpected Columns",
                message=f"Found {len(unexpected)} column(s) not in master schema",
                details=unexpected,
                file_name=file_name
            ))

    def _check_missing_values(self, df: pd.DataFrame, file_name: str) -> None:
        """Check for missing values in critical fields."""
        for col in self.required_fields:
            if col not in df.columns:
                continue
            if df[col].dtype == object:
                mask = (df[col].isna() | (df[col] == "") | (df[col] == "nan"))
            else:
                mask = df[col].isna()
            missing_count = mask.sum()
            if missing_count > 0:
                sample_rows = df[mask].index.tolist()[:5]
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
        """Check for duplicate records using merge key logic."""
        merge_keys = self._create_merge_keys(df)
        df["_merge_key"] = merge_keys
        duplicates = df[df.duplicated(subset=["_merge_key"], keep=False)]
        if len(duplicates) > 0:
            dup_keys = duplicates["_merge_key"].unique().tolist()[:5]
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Duplicate Records",
                message=f"Found {len(duplicates)} rows with duplicate merge keys",
                details=[f"Example keys: {dup_keys}"],
                file_name=file_name,
                row_count=len(duplicates)
            ))
        df.drop(columns=["_merge_key"], inplace=True, errors="ignore")

    def _check_numeric_ranges(self, df: pd.DataFrame, file_name: str) -> None:
        """Validate numeric fields are in reasonable ranges."""
        for col in self.numeric_fields:
            if col not in df.columns:
                continue

            # Coerce to numeric once here for safety (handles manually edited files)
            s = pd.to_numeric(df[col], errors="coerce")

            # Check for negative values
            negatives = s[s < 0]
            if len(negatives) > 0:
                self.results.append(ValidationResult(
                    passed=False,
                    severity="ERROR",
                    check_name=f"Negative Values - {col}",
                    message=f"Found {len(negatives)} row(s) with negative {col}",
                    file_name=file_name,
                    row_count=len(negatives)
                ))

            # Check for unreasonably high values in student count columns
            if "Student" in col or "Students" in col:
                high = s[s > self.max_students]
                if len(high) > 0:
                    self.results.append(ValidationResult(
                        passed=True,
                        severity="WARNING",
                        check_name=f"High Values - {col}",
                        message=f"Found {len(high)} row(s) with {col} > {self.max_students}",
                        file_name=file_name,
                        row_count=len(high)
                    ))

    def _check_logical_consistency(self, df: pd.DataFrame, file_name: str) -> None:
        """Check mathematical relationships between columns."""
        required = ["Total Student Count", "# of Students Opt In", "# of Students Opt Out", "# of Students No Response"]
        if not all(c in df.columns for c in required):
            return

        # Check that counts sum to total
        df_check = df[required].copy()
        for col in required:
            df_check[col] = pd.to_numeric(df_check[col], errors="coerce").fillna(0)

        df_check["sum"] = df_check["# of Students Opt In"] + df_check["# of Students Opt Out"] + df_check["# of Students No Response"]
        mismatches = df_check[df_check["Total Student Count"] != df_check["sum"]]

        if len(mismatches) > 0:
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Count Mismatch",
                message=f"Found {len(mismatches)} row(s) where opt-in + opt-out + no-response != total",
                file_name=file_name,
                row_count=len(mismatches)
            ))

    def _check_approval_status_values(self, df: pd.DataFrame, file_name: str) -> None:
        """Validate approval status values against whitelist."""
        if "Approval Status" not in df.columns:
            return
        invalid = df[~df["Approval Status"].isin(self.valid_statuses) & (df["Approval Status"] != "")]
        if len(invalid) > 0:
            invalid_vals = invalid["Approval Status"].unique().tolist()[:5]
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Invalid Approval Status",
                message=f"Found {len(invalid)} row(s) with non-standard approval status",
                details=[f"Values: {invalid_vals}"],
                file_name=file_name,
                row_count=len(invalid)
            ))

    def _check_percentage_validity(self, df: pd.DataFrame, file_name: str) -> None:
        """Validate percentage fields are 0-100."""
        for col in self.percentage_fields:
            if col not in df.columns:
                continue
            df_pct = pd.to_numeric(df[col], errors="coerce")
            out_of_range = df[(df_pct < 0) | (df_pct > 100)]
            if len(out_of_range) > 0:
                self.results.append(ValidationResult(
                    passed=False,
                    severity="ERROR",
                    check_name=f"Invalid Percentage - {col}",
                    message=f"Found {len(out_of_range)} row(s) with {col} outside 0-100",
                    file_name=file_name,
                    row_count=len(out_of_range)
                ))

    def _check_report_date(self, df: pd.DataFrame, file_name: str) -> None:
        """Validate report date consistency."""
        if "Report Date" not in df.columns:
            return

        # Get first non-empty date
        dates = df["Report Date"].dropna()
        if len(dates) == 0:
            return

        first_date_str = str(dates.iloc[0]).strip()
        if not first_date_str:
            return

        # Try to parse
        try:
            col_date = datetime.strptime(first_date_str, "%m-%d-%Y").date()
        except ValueError:
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Invalid Report Date",
                message=f"Report Date '{first_date_str}' not parseable as MM-DD-YYYY",
                file_name=file_name
            ))
            return

        # Compare to filename
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
        if col_date > date.today():
            self.results.append(ValidationResult(
                passed=False,
                severity="ERROR",
                check_name="Future Report Date",
                message=f"Report date {col_date} is in the future",
                file_name=file_name
            ))

        # Warn on old dates
        one_year_ago = date.today() - timedelta(days=365)
        if col_date < one_year_ago:
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Old Report Date",
                message=f"Report date {col_date} is more than 1 year old",
                file_name=file_name
            ))

    def _check_school_names(self, df: pd.DataFrame, file_name: str) -> None:
        """Check for potential issues in school names."""
        if "School of Instruction" not in df.columns:
            return

        # Check for very short names (might be codes instead of names)
        short_names = df[df["School of Instruction"].str.len() < 3]
        if len(short_names) > 0:
            examples = short_names["School of Instruction"].unique().tolist()[:5]
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Short School Names",
                message=f"Found {len(short_names)} row(s) with very short school names",
                details=[f"Examples: {examples}"],
                file_name=file_name
            ))

    def _check_unusual_patterns(self, df: pd.DataFrame, file_name: str) -> None:
        """Detect unusual data patterns."""
        # Check for all-zeros rows
        count_cols = ["# of Students Opt In", "# of Students Opt Out", "# of Students No Response"]
        if all(c in df.columns for c in count_cols):
            df_counts = df[count_cols].copy()
            for col in count_cols:
                df_counts[col] = pd.to_numeric(df_counts[col], errors="coerce").fillna(0)
            all_zero = (df_counts.sum(axis=1) == 0)
            total_col = pd.to_numeric(df.get("Total Student Count", 0), errors="coerce").fillna(0)
            suspicious = df[all_zero & (total_col > 0)]
            if len(suspicious) > 0:
                self.results.append(ValidationResult(
                    passed=True,
                    severity="WARNING",
                    check_name="Zero Response Pattern",
                    message=f"Found {len(suspicious)} row(s) with students but no responses",
                    file_name=file_name,
                    row_count=len(suspicious)
                ))

    # ========================================================================
    # CROSS-FILE CHECKS
    # ========================================================================

    def run_cross_file_checks(self) -> None:
        """Compare files to detect anomalies across reports."""
        if len(self.processed_files_data) < 2:
            logging.info("Skipping cross-file checks (need at least 2 files)")
            return

        logging.info("\nRunning cross-file anomaly detection...")
        sorted_files = sorted(self.processed_files_data.keys())

        for i in range(1, len(sorted_files)):
            prev_file = sorted_files[i - 1]
            curr_file = sorted_files[i]
            prev_df = self.processed_files_data[prev_file]
            curr_df = self.processed_files_data[curr_file]

            self._check_vanished_classrooms(prev_df, curr_df, prev_file, curr_file)
            self._check_count_swings(prev_df, curr_df, prev_file, curr_file)
            self._check_approval_regressions(prev_df, curr_df, prev_file, curr_file)

    def _check_vanished_classrooms(self, prev_df: pd.DataFrame, curr_df: pd.DataFrame,
                                    prev_file: str, curr_file: str) -> None:
        """Find classrooms that disappeared between reports."""
        prev_keys = set(self._create_merge_keys(prev_df))
        curr_keys = set(self._create_merge_keys(curr_df))
        vanished = prev_keys - curr_keys

        if vanished:
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Vanished Classrooms",
                message=f"{len(vanished)} classroom(s) in {prev_file} missing from {curr_file}",
                details=[f"Examples: {list(vanished)[:5]}"],
                file_name=curr_file,
                row_count=len(vanished)
            ))

    def _check_count_swings(self, prev_df: pd.DataFrame, curr_df: pd.DataFrame,
                            prev_file: str, curr_file: str) -> None:
        """Detect large swings in student counts."""
        if "Total Student Count" not in prev_df.columns or "Total Student Count" not in curr_df.columns:
            return

        prev_df = prev_df.copy()
        curr_df = curr_df.copy()
        prev_df["_merge_key"] = self._create_merge_keys(prev_df)
        curr_df["_merge_key"] = self._create_merge_keys(curr_df)

        merged = prev_df[["_merge_key", "Total Student Count"]].merge(
            curr_df[["_merge_key", "Total Student Count"]],
            on="_merge_key",
            suffixes=("_prev", "_curr")
        )

        for col in ["Total Student Count_prev", "Total Student Count_curr"]:
            merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)

        # Calculate percentage change
        merged["pct_change"] = abs(merged["Total Student Count_curr"] - merged["Total Student Count_prev"]) / merged["Total Student Count_prev"].replace(0, 1) * 100
        merged["abs_delta"] = abs(merged["Total Student Count_curr"] - merged["Total Student Count_prev"])

        # Filter for significant swings
        large_swings = merged[(merged["pct_change"] > self.swing_threshold) & (merged["abs_delta"] >= self.min_student_delta)]

        if len(large_swings) > 0:
            # Create readable examples with delta and direction (LOGAN FIX)
            example_list = []
            for _, r in large_swings.head(5).iterrows():
                key = r["_merge_key"]
                delta = int(r["Total Student Count_curr"] - r["Total Student Count_prev"])
                sign = "↑" if delta > 0 else "↓"
                example_list.append(f"{key} (Δ={delta:+d} {sign}, {round(r['pct_change'], 1)}%)")

            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Large Count Swings",
                message=(
                    f"Found {len(large_swings)} classroom(s) with >{self.swing_threshold}% change "
                    f"in total student count (and ≥{self.min_student_delta} student delta)"
                ),
                details=[f"Examples: {example_list}"],
                file_name=curr_file,
                row_count=len(large_swings)
            ))

    def _check_approval_regressions(self, prev_df: pd.DataFrame, curr_df: pd.DataFrame,
                                     prev_file: str, curr_file: str) -> None:
        """Detect approval status regressions (Approved -> Not Approved)."""
        if "Approval Status" not in prev_df.columns or "Approval Status" not in curr_df.columns:
            return

        prev_df = prev_df.copy()
        curr_df = curr_df.copy()
        prev_df["_merge_key"] = self._create_merge_keys(prev_df)
        curr_df["_merge_key"] = self._create_merge_keys(curr_df)

        merged = prev_df[["_merge_key", "Approval Status"]].merge(
            curr_df[["_merge_key", "Approval Status"]],
            on="_merge_key",
            suffixes=("_prev", "_curr")
        )

        approved = "Approved - Camera Authorized"
        regressions = merged[
            (merged["Approval Status_prev"] == approved) &
            (merged["Approval Status_curr"] != approved)
        ]

        if len(regressions) > 0:
            examples = regressions["_merge_key"].tolist()[:5]
            self.results.append(ValidationResult(
                passed=True,
                severity="WARNING",
                check_name="Approval Regressions",
                message=f"{len(regressions)} classroom(s) lost approval between {prev_file} and {curr_file}",
                details=[f"Examples: {examples}"],
                file_name=curr_file,
                row_count=len(regressions)
            ))

    # ========================================================================
    # ARTIFACT GENERATION
    # ========================================================================

    def generate_artifacts(self) -> None:
        """Generate validation report CSV files."""
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

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
            logging.info("No validation issues - no artifacts generated")

    def create_validation_markers(self, files: List[Path]) -> None:
        """Create .validated_ok marker files for passed files."""
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
            print("[PASSED] ALL CHECKS PASSED")
            print("  No data quality issues detected.")
        else:
            if errors:
                print(f"\n[ERRORS] {len(errors)} critical issue(s) found:")
                for e in errors:
                    print(f"  - [{e.check_name}] {e.file_name}: {e.message}")
                    for detail in e.details:
                        print(f"      {detail}")

            if warnings:
                print(f"\n[WARNINGS] {len(warnings)} warning(s) found:")
                for w in warnings:
                    print(f"  - [{w.check_name}] {w.file_name}: {w.message}")

        return len(errors), len(warnings)


# ============================================================================
# MAIN ENTRY POINT
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

    # FIX 1: Load config first to get log_dir path (not hardcoded)
    config_path = Path(args.config)
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
        log_dir_str = config.get("paths", {}).get("log_dir", "Logs")
    else:
        log_dir_str = "Logs"

    # Setup logging using config path
    log_dir = Path(log_dir_str).expanduser().resolve()
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