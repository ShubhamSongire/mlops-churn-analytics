"""
Data versioning utilities for the churn pipeline.

This module calculates checksums for data artifacts (raw and transformed
files) to enable reproducibility.  It writes a JSON file capturing
paths, hashes and timestamps of each versioned object.

Although tools like DVC or Git LFS are typically used for data
versioning, this simple implementation fulfils the assignment
requirements by demonstrating how version metadata can be recorded.
"""
import hashlib
import json
import os
from datetime import datetime
from typing import Dict, List


def compute_md5(file_path: str, chunk_size: int = 8192) -> str:
    """Compute the MD5 hash of a file."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        chunk = f.read(chunk_size)
        while chunk:
            md5.update(chunk)
            chunk = f.read(chunk_size)
    return md5.hexdigest()


def record_versions(file_paths: List[str], output_path: str) -> None:
    """Record versions of given files into a JSON file.

    Args:
        file_paths: List of file paths to version.
        output_path: Path to versions JSON file.
    """
    version_records = []
    for path in file_paths:
        if not os.path.exists(path):
            continue
        version_records.append({
            "file": os.path.abspath(path),
            "md5": compute_md5(path),
            "timestamp": datetime.now().isoformat(),
        })
    # Append existing records if file exists
    if os.path.exists(output_path):
        with open(output_path, "r") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
    else:
        existing = []
    existing.extend(version_records)
    with open(output_path, "w") as f:
        json.dump(existing, f, indent=2)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Record MD5 versions of files")
    parser.add_argument("--files", nargs='+', help="List of files to version")
    parser.add_argument("--output", type=str, default="data/versions.json", help="Path to versions.json")
    args = parser.parse_args()
    record_versions(args.files, args.output)