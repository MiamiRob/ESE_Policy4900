# run_policy4900_pipeline.py

"""
Policy 4900 Processing Pipeline Orchestrator
============================================
Runs Task1 and Task2 in sequence with error handling and flexible execution modes.

USAGE:
  python run_policy4900_pipeline.py              # Run both Task1 → Task2
  python run_policy4900_pipeline.py --step1      # Run Task1 only (process CSVs)
  python run_policy4900_pipeline.py --step2      # Run Task2 only (merge to master)
  python run_policy4900_pipeline.py --help       # Show this help

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


def main():
    parser = argparse.ArgumentParser(
        description="ESE Policy 4900 Processing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EXAMPLES:
  # Normal operation - run both steps with validation (RECOMMENDED)
  python run_policy4900_pipeline.py

  # Process new CSV reports only (includes validation)
  python run_policy4900_pipeline.py --step1

  # Skip validation (not recommended for production)
  python run_policy4900_pipeline.py --no-validate

  # Merge previously processed files only (no validation)
  python run_policy4900_pipeline.py --step2

  # Run validation only on existing processed files
  python run_policy4900_pipeline.py --validate

  # Review workflow (recommended for production)
  python run_policy4900_pipeline.py --step1    # Process + validate CSVs
  # <manually review *_PROCESSED.csv files>
  python run_policy4900_pipeline.py --step2    # Merge after review
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

    args = parser.parse_args()

    # Locate script files (same directory as this orchestrator)
    here = Path(__file__).resolve().parent
    task1_script = here / "task1_process_reports.py"
    task2_script = here / "task2_merge_to_master_with_timeline.py"
    validate_script = here / "validate_processed_data.py"

    print_banner("ESE Policy 4900 Processing Pipeline")
    print(f"Orchestrator: {Path(__file__).name}")
    print(f"Working Dir:  {here}")
    print(f"Python:       {sys.executable}")
    print(f"Start Time:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

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
            print("Review recommended, but Task2 will proceed.")
            if run_step2:
                input("\nPress Enter to continue with Task2, or Ctrl+C to abort...")

    # Execute Task2 if requested (only after Task1/validation succeed)
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