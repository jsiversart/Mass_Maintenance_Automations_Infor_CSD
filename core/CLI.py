# cli.py
import argparse
import subprocess
import sys
from pathlib import Path



BASE_DIR = Path("mass_jobs")
# change to absolute or relative to repo root
BASE_DIR = Path(__file__).resolve().parent.parent / "mass_jobs"

# -------------------------
# Helpers
# -------------------------
def get_job_path(job_name):
    job_path = BASE_DIR / job_name
    if not job_path.exists():
        print(f"❌ Job '{job_name}' not found.")
        sys.exit(1)
    return job_path


def run_script(script_name, job_path):
    result = subprocess.run(
        ["python", script_name],
        cwd=job_path
    )
    if result.returncode != 0:
        print(f"❌ {script_name} failed.")
        sys.exit(1)


# -------------------------
# Commands
# -------------------------
def run_pt1(job_name):
    job_path = get_job_path(job_name)

    print(f"\n🚀 Running Part 1 for '{job_name}'...\n")
    run_script("pt1.py", job_path)

    print("\n✅ Part 1 complete.")
    print("\nNext steps:")
    print("1. Upload generated file into ERP")
    print("2. Run mass maintenance export")
    print(f"3. Save export file into: {job_path / 'data'}")
    print(f"\nThen run:\n   massmaint pt2 {job_name}\n")


def run_pt2(job_name):
    job_path = get_job_path(job_name)

    print(f"\n🚀 Running Part 2 for '{job_name}'...\n")
    run_script("pt2.py", job_path)

    print("\n✅ Part 2 complete.\n")


def run_all(job_name):
    run_pt1(job_name)

    input("\n⏸️ Press ENTER after placing export file in data folder...")

    run_pt2(job_name)


def list_jobs():
    print("\n📦 Available jobs:\n")
    for job in BASE_DIR.iterdir():
        if job.is_dir():
            print(f" - {job.name}")
    print()


# -------------------------
# Argparse Setup
# -------------------------
def main():
    parser = argparse.ArgumentParser(
        prog="massmaint",
        description="Mass Maintenance CLI Tool"
    )

    subparsers = parser.add_subparsers(dest="command")

    # pt1
    pt1_parser = subparsers.add_parser("pt1", help="Run part 1")
    pt1_parser.add_argument("job")

    # pt2
    pt2_parser = subparsers.add_parser("pt2", help="Run part 2")
    pt2_parser.add_argument("job")

    # run
    run_parser = subparsers.add_parser("run", help="Run pt1 + pt2")
    run_parser.add_argument("job")

    # list
    subparsers.add_parser("list", help="List jobs")

    args = parser.parse_args()

    if args.command == "pt1":
        run_pt1(args.job)
    elif args.command == "pt2":
        run_pt2(args.job)
    elif args.command == "run":
        run_all(args.job)
    elif args.command == "list":
        list_jobs()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()