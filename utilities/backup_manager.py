# backup_manager.py
"""
Backup Manager for ESE Policy 4900 Master Files
Allows viewing, restoring, and managing backup files
Project Path: C:\\users\\rlzim\\PycharmProjects\\ESE_Policy4900
"""

import shutil
from pathlib import Path
from datetime import datetime, timedelta
from colorama import init, Fore, Style
import pandas as pd

init(autoreset=True)


class BackupManager:
    def __init__(self):
        self.backup_dir = Path(r"C:\BCPS\ESE\Backups\Policy4900_Reports_Master")
        self.master_dir = Path(r"C:\BCPS\ESE\Policy4900_Reports_Master")

        # Ensure directories exist
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self.master_dir.mkdir(parents=True, exist_ok=True)

    def list_backups(self):
        """List all backup files with details"""
        backups = list(self.backup_dir.glob("Policy4900_Master_Backup_*.xlsx"))

        if not backups:
            print(f"{Fore.YELLOW}No backups found in {self.backup_dir}{Style.RESET_ALL}")
            return []

        # Sort by timestamp in filename
        backups.sort(reverse=True)  # Most recent first

        backup_info = []
        for backup in backups:
            # Extract timestamp from filename
            try:
                # Format: Policy4900_Master_Backup_YYYYMMDD_HHMMSS.xlsx
                parts = backup.stem.split('_')
                date_str = parts[-2]  # YYYYMMDD
                time_str = parts[-1]  # HHMMSS

                # Parse the timestamp
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                hour = int(time_str[:2])
                minute = int(time_str[2:4])
                second = int(time_str[4:6])

                timestamp = datetime(year, month, day, hour, minute, second)

                # Get file size
                size_mb = backup.stat().st_size / (1024 * 1024)

                backup_info.append({
                    'path': backup,
                    'timestamp': timestamp,
                    'size_mb': size_mb,
                    'age_days': (datetime.now() - timestamp).days
                })
            except:
                continue

        return backup_info

    def display_backups(self):
        """Display all backups in a formatted table"""
        print(f"\n{Fore.CYAN}{'=' * 70}")
        print(f"{'Master File Backups':^70}")
        print(f"{'=' * 70}{Style.RESET_ALL}\n")

        backups = self.list_backups()

        if not backups:
            return

        print(f"Found {len(backups)} backup(s) in: {self.backup_dir}\n")

        for i, backup in enumerate(backups, 1):
            age_color = Fore.GREEN if backup['age_days'] < 7 else Fore.YELLOW if backup['age_days'] < 30 else Fore.RED

            print(f"{i}. {Fore.CYAN}{backup['path'].name}{Style.RESET_ALL}")
            print(f"   Date: {backup['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   Size: {backup['size_mb']:.2f} MB")
            print(f"   Age:  {age_color}{backup['age_days']} days old{Style.RESET_ALL}")
            print()

        return backups

    def restore_backup(self, backup_number=None):
        """Restore a backup to the master directory"""
        backups = self.display_backups()

        if not backups:
            return

        if backup_number is None:
            try:
                choice = input(f"Enter backup number to restore (1-{len(backups)}): ")
                backup_number = int(choice)
            except:
                print(f"{Fore.RED}Invalid choice{Style.RESET_ALL}")
                return

        if not 1 <= backup_number <= len(backups):
            print(f"{Fore.RED}Invalid backup number{Style.RESET_ALL}")
            return

        backup = backups[backup_number - 1]

        # Generate new master filename with current date
        today = datetime.now().strftime('%m%d%Y')
        new_master_name = f"Policy4900_Tracking_Master_Updated_{today}.xlsx"
        new_master_path = self.master_dir / new_master_name

        # Check if it already exists
        if new_master_path.exists():
            print(f"{Fore.YELLOW}Warning: {new_master_name} already exists{Style.RESET_ALL}")
            overwrite = input("Overwrite? (y/n): ").lower()
            if overwrite != 'y':
                print("Restore cancelled")
                return

        # Copy the backup to master
        try:
            shutil.copy2(backup['path'], new_master_path)
            print(f"{Fore.GREEN}✓ Restored backup to: {new_master_name}{Style.RESET_ALL}")
            print(f"  From backup: {backup['path'].name}")
            print(f"  Original date: {backup['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            print(f"{Fore.RED}Error restoring backup: {e}{Style.RESET_ALL}")

    def cleanup_old_backups(self, days_to_keep=30):
        """Remove backups older than specified days"""
        backups = self.list_backups()

        old_backups = [b for b in backups if b['age_days'] > days_to_keep]

        if not old_backups:
            print(f"{Fore.GREEN}No backups older than {days_to_keep} days{Style.RESET_ALL}")
            return

        print(f"\n{Fore.YELLOW}Found {len(old_backups)} backup(s) older than {days_to_keep} days:{Style.RESET_ALL}")
        for backup in old_backups:
            print(f"  - {backup['path'].name} ({backup['age_days']} days old)")

        confirm = input(f"\nDelete these {len(old_backups)} old backup(s)? (y/n): ").lower()

        if confirm == 'y':
            for backup in old_backups:
                try:
                    backup['path'].unlink()
                    print(f"{Fore.GREEN}✓ Deleted: {backup['path'].name}{Style.RESET_ALL}")
                except Exception as e:
                    print(f"{Fore.RED}Error deleting {backup['path'].name}: {e}{Style.RESET_ALL}")

    def compare_backups(self):
        """Compare two backups to see what changed"""
        backups = self.display_backups()

        if len(backups) < 2:
            print(f"{Fore.YELLOW}Need at least 2 backups to compare{Style.RESET_ALL}")
            return

        try:
            older = int(input(f"Enter OLDER backup number (1-{len(backups)}): "))
            newer = int(input(f"Enter NEWER backup number (1-{len(backups)}): "))

            if not (1 <= older <= len(backups) and 1 <= newer <= len(backups)):
                print(f"{Fore.RED}Invalid backup numbers{Style.RESET_ALL}")
                return

            # Load both files
            print(f"\n{Fore.CYAN}Loading files for comparison...{Style.RESET_ALL}")
            df_old = pd.read_excel(backups[older - 1]['path'], sheet_name='Policy 4900 - Classroom Percent')
            df_new = pd.read_excel(backups[newer - 1]['path'], sheet_name='Policy 4900 - Classroom Percent')

            # Compare
            print(f"\n{Fore.CYAN}Comparison Results:{Style.RESET_ALL}")
            print(f"Older: {backups[older - 1]['path'].name}")
            print(f"Newer: {backups[newer - 1]['path'].name}")
            print(f"\nOlder file: {len(df_old)} rows")
            print(f"Newer file: {len(df_new)} rows")
            print(f"Difference: {len(df_new) - len(df_old)} rows")

            # Count approved cameras
            old_approved = (df_old['Approval Status'] == 'Approved - Camera Authorized').sum()
            new_approved = (df_new['Approval Status'] == 'Approved - Camera Authorized').sum()

            print(f"\nApproved cameras:")
            print(f"  Older: {old_approved}")
            print(f"  Newer: {new_approved}")
            print(f"  Change: {new_approved - old_approved:+d}")

        except Exception as e:
            print(f"{Fore.RED}Error comparing backups: {e}{Style.RESET_ALL}")


def main():
    """Main menu for backup management"""
    manager = BackupManager()

    while True:
        print(f"\n{Fore.CYAN}{'=' * 50}")
        print(f"{'Backup Manager':^50}")
        print(f"{'=' * 50}{Style.RESET_ALL}")
        print("\n1. List all backups")
        print("2. Restore a backup")
        print("3. Compare two backups")
        print("4. Clean up old backups (>30 days)")
        print("5. Exit")

        choice = input("\nSelect option (1-5): ").strip()

        if choice == '1':
            manager.display_backups()
        elif choice == '2':
            manager.restore_backup()
        elif choice == '3':
            manager.compare_backups()
        elif choice == '4':
            manager.cleanup_old_backups()
        elif choice == '5':
            print(f"{Fore.CYAN}Exiting...{Style.RESET_ALL}")
            break
        else:
            print(f"{Fore.YELLOW}Invalid choice{Style.RESET_ALL}")

        if choice != '5':
            input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")


if __name__ == "__main__":
    main()