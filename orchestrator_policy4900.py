# orchestrator_policy4900.py

"""
Policy 4900 Processing Pipeline Orchestrator
============================================
Runs Task1 and Task2 in sequence with error handling and flexible execution modes.

USAGE:
  python orchestrator_policy4900.py              # Run both Task1 → Task2
  python orchestrator_policy4900.py --step1      # Run Task1 only (process CSVs)
  python orchestrator_policy4900.py --step2      # Run Task2 only (merge to master)
  python orchestrator_policy4900.py --dry-run    # Preview what would be processed (no changes)
  python orchestrator_policy4900.py --help       # Show this help

WORKFLOW:
  Task1: Process raw *_Policy4900.csv → *_Policy4900_PROCESSED.csv + .step1 markers
  Task2: Merge processed files → Policy4900_Tracking_Master.xlsx + .step2 markers

WHY SEPARATE SCRIPTS:
  - Task1 never touches the master Excel file (safety boundary)
  - Checkpoint after Task1 lets you review processed CSVs before merge
  - Independent logging for each step (easier troubleshooting)
  - Can re-run either step independently without affecting the other
  - Task2's backup/retention system expects Task1's output format

AUTHOR: Rob Zimmerman
DATE: 2025-Nov-08
"""

import argparse
import subprocess
import sys
from pathlib import Path
from datetime import datetime
import json


def print_banner(message: str):
    """Print a formatted banner for visibility."""
    border = "=" * 70
    print(f"\n{border}")
    print(f"  {message}")
    print(f"{border}\n")


def run_script(script_path: Path, step_name: str) -> int:
    """
    Execute a Python script and return its exit code.

    Args:
        script_path: Path to the Python script to run
        step_name: Human-readable name for logging (e.g., "Task1")

    Returns:
        Exit code (0 = success, non-zero = failure)
    """
    if not script_path.exists():
        print(f"[ERROR] {step_name} script not found: {script_path}")
        return 1

    print_banner(f"Starting {step_name}: {script_path.name}")
    start_time = datetime.now()

    # Use same Python interpreter that's running this orchestrator
    cmd = [sys.executable, str(script_path)]
    print(f"[EXEC] {' '.join(cmd)}")

    try:
        proc = subprocess.run(
            cmd,
            cwd=script_path.parent,  # Run in script's directory
            check=False  # Don't raise exception, we'll handle return code
        )

        elapsed = (datetime.now() - start_time).total_seconds()

        if proc.returncode == 0:
            print(f"\n[✓] {step_name} completed successfully in {elapsed:.1f}s")
        else:
            print(f"\n[✗] {step_name} FAILED with exit code {proc.returncode} after {elapsed:.1f}s")

        return proc.returncode

    except Exception as e:
        print(f"\n[✗] {step_name} crashed with exception: {e}")
        return 1


def run_dry_run(config_path: Path):
    """
    Preview what would be processed without making any changes.
    Shows files ready for Task1 and Task2 processing.
    """
    print_banner("DRY RUN - Preview Mode (No Changes Will Be Made)")

    # Load config
    if not config_path.exists():
        print(f"[ERROR] Config file not found: {config_path}")
        return 1

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    paths = config.get("paths", {})
    new_reports_dir = Path(paths.get("new_reports_dir", ".")).expanduser().resolve()
    master_dir = Path(paths.get("master_dir", ".")).expanduser().resolve()
    master_file = master_dir / paths.get("master_file", "Policy4900_Tracking_Master.xlsx")

    print(f"Reports Directory: {new_reports_dir}")
    print(f"Master File: {master_file}")
    print()

    # Check for files ready for Task1 (raw CSVs without .step1 marker)
    print("-" * 70)
    print("TASK 1 - Files Ready for Processing (raw CSVs without .step1 marker)")
    print("-" * 70)

    if not new_reports_dir.exists():
        print(f"[ERROR] Reports directory does not exist: {new_reports_dir}")
    else:
        pattern = "*_Policy4900.csv"
        all_csvs = sorted(new_reports_dir.glob(pattern))

        # Filter out PROCESSED files
        raw_csvs = [f for f in all_csvs if "_PROCESSED" not in f.name]

        task1_ready = []
        task1_done = []

        for csv_path in raw_csvs:
            step1_marker = csv_path.parent / f"{csv_path.name}.step1"
            if step1_marker.exists():
                task1_done.append(csv_path)
            else:
                task1_ready.append(csv_path)

        if task1_ready:
            print(f"\n[PENDING] {len(task1_ready)} file(s) ready for Task1:")
            for f in task1_ready:
                print(f"  → {f.name}")
        else:
            print("\n[OK] No files pending for Task1")

        if task1_done:
            print(f"\n[DONE] {len(task1_done)} file(s) already processed by Task1:")
            for f in task1_done[:5]:  # Show first 5
                print(f"  ✓ {f.name}")
            if len(task1_done) > 5:
                print(f"  ... and {len(task1_done) - 5} more")

    # Check for files ready for Task2 (processed CSVs with .step1 but without .step2)
    print()
    print("-" * 70)
    print("TASK 2 - Files Ready for Merge (processed CSVs with .step1, without .step2)")
    print("-" * 70)

    if not new_reports_dir.exists():
        print(f"[ERROR] Reports directory does not exist")
    else:
        pattern = "*_Policy4900_PROCESSED.csv"
        all_processed = sorted(new_reports_dir.glob(pattern))

        task2_ready = []
        task2_done = []

        for csv_path in all_processed:
            original_name = csv_path.name.replace("_PROCESSED.csv", ".csv")
            step1_marker = csv_path.parent / f"{original_name}.step1"
            step2_marker = csv_path.parent / f"{original_name}.step2"

            if step1_marker.exists() and not step2_marker.exists():
                task2_ready.append(csv_path)
            elif step2_marker.exists():
                task2_done.append(csv_path)

        if task2_ready:
            print(f"\n[PENDING] {len(task2_ready)} file(s) ready for Task2:")
            for f in task2_ready:
                print(f"  → {f.name}")
        else:
            print("\n[OK] No files pending for Task2")

        if task2_done:
            print(f"\n[DONE] {len(task2_done)} file(s) already merged by Task2:")
            for f in task2_done[:5]:  # Show first 5
                print(f"  ✓ {f.name}")
            if len(task2_done) > 5:
                print(f"  ... and {len(task2_done) - 5} more")

    # Master file status
    print()
    print("-" * 70)
    print("MASTER FILE STATUS")
    print("-" * 70)

    if master_file.exists():
        import os
        stat = os.stat(master_file)
        mod_time = datetime.fromtimestamp(stat.st_mtime)
        size_kb = stat.st_size / 1024
        print(f"\n[EXISTS] {master_file.name}")
        print(f"  Size: {size_kb:.1f} KB")
        print(f"  Last Modified: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print(f"\n[NEW] Master file does not exist yet - will be created on first Task2 run")

    print()
    print_banner("DRY RUN COMPLETE - No changes were made")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="ESE Policy 4900 Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Normal operation - run both steps with validation (RECOMMENDED)
  python orchestrator_policy4900.py

  # Preview what would be processed (no changes made)
  python orchestrator_policy4900.py --dry-run

  # Process new CSV reports only (includes validation)
  python orchestrator_policy4900.py --step1

  # Skip validation (not recommended for production)
  python orchestrator_policy4900.py --no-validate

  # Merge previously processed files only (no validation)
  python orchestrator_policy4900.py --step2

  # Run validation only on existing processed files
  python orchestrator_policy4900.py --validate

  # Review workflow (recommended for production)
  python orchestrator_policy4900.py --step1    # Process + validate CSVs
  # <manually review *_PROCESSED.csv files>
  python orchestrator_policy4900.py --step2    # Merge after review
        """
    )

    parser.add_argument(
        "--step1",
        action="store_true",
        help="Run Task1 only (process raw CSV reports)"
    )
    parser.add_argument(
        "--step2",
        action="store_true",
        help="Run Task2 only (merge to master workbook)"
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run validation checks between Task1 and Task2 (recommended)"
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip validation checks (not recommended for production)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be processed without making any changes"
    )

    args = parser.parse_args()

    # Locate script files (same directory as this orchestrator)
    here = Path(__file__).resolve().parent
    task1_script = here / "task1_process_reports.py"
    task2_script = here / "task2_merge_to_master_with_timeline.py"
    validate_script = here / "validate_processed_data.py"
    config_path = here / "config.json"

    print_banner("ESE Policy 4900 Processing Pipeline")
    print(f"Orchestrator: {Path(__file__).name}")
    print(f"Working Dir:  {here}")
    print(f"Python:       {sys.executable}")
    print(f"Start Time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Handle dry-run mode
    if args.dry_run:
        exit_code = run_dry_run(config_path)
        sys.exit(exit_code)

    # Determine execution mode
    run_step1 = args.step1 or (not args.step1 and not args.step2)
    run_step2 = args.step2 or (not args.step1 and not args.step2)

    # Determine if validation should run (default is YES unless explicitly disabled)
    run_validation = (not args.no_validate) and (run_step1 or args.validate)

    exit_code = 0

    # Execute Task1 if requested
    if run_step1:
        exit_code = run_script(task1_script, "Task1 - Process Reports")

        if exit_code != 0:
            print_banner("PIPELINE HALTED - Task1 Failed")
            print("Fix Task1 errors before running Task2.")
            print("Check the Task1 log file in your Logs directory for details.")
            sys.exit(exit_code)

        # Execute validation if requested (after Task1 succeeds)
        if run_validation:
            exit_code = run_script(validate_script, "Validation - Data Quality Checks")

            if exit_code == 1:
                # Critical errors - block Task2
                print_banner("PIPELINE HALTED - Validation Failed")
                print("Critical data quality errors detected.")
                print("Fix the issues in the processed CSV files before running Task2.")
                sys.exit(1)
            elif exit_code == 2:
                # Warnings only - inform but don't block
                print_banner("Validation Warnings Detected")
                print("Data quality warnings found (see above).")
                print("Review recommended, but Task2 will proceed automatically.\n")

    # Execute Task2 if requested (FIXED: now outside the run_step1 block)
    if run_step2:
        exit_code = run_script(task2_script, "Task2 - Merge to Master")

        if exit_code != 0:
            print_banner("PIPELINE HALTED - Task2 Failed")
            print("Check the Task2 log file in your Logs directory for details.")
            sys.exit(exit_code)

    # Success message
    print_banner("PIPELINE COMPLETED SUCCESSFULLY")

    if run_step1 and run_step2:
        print("✓ Task1: Raw reports processed → *_PROCESSED.csv files")
        if run_validation:
            print("✓ Validation: Data quality checks passed")
        print("✓ Task2: Processed files merged → Policy4900_Tracking_Master.xlsx")
    elif run_step1:
        print("✓ Task1: Raw reports processed → *_PROCESSED.csv files")
        if run_validation:
            print("✓ Validation: Data quality checks passed")
        print("  Next: Review processed files, then run with --step2")
    elif run_step2:
        print("✓ Task2: Processed files merged → Policy4900_Tracking_Master.xlsx")

    print(f"\nEnd Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    sys.exit(0)


if __name__ == "__main__":
    main()