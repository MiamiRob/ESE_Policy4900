# VALIDATION SCRIPT FIXES
# =======================
# Two changes needed in validate_processed_data.py

# ============================================================================
# FIX 1: Replace lines 940-983 (main function logging setup)
# Change from hardcoded path to config-based path
# ============================================================================

# REPLACE THIS (lines 940-983):
"""
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
"""

# WITH THIS:
"""
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

    # Load config first to get log_dir path
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
"""

# ============================================================================
# FIX 2: Remove duplicate line at 787 in _check_count_swings method
# ============================================================================

# REPLACE THIS (lines 778-799):
"""
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
"""

# WITH THIS (removed duplicate append on lines 786-787):
"""
        if len(large_swings) > 0:
            # Create readable examples with % and Δ count
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
                    f"in total student count (and ≥{self.config['validation'].get('min_student_delta', 2)} student delta)"
                ),
                details=[f"Examples: {example_list}"],
                file_name=curr_file
            ))
"""

# NOTE: The second fix also improves the delta display:
# - Uses {delta:+d} format to show +5 or -3 (signed integer)
# - Combined with the arrow (↑/↓) gives clear visual indication
# - Single line per classroom instead of two