#!/usr/bin/env python3
"""Chunk S3 image objects into chunk-* folders using rclone.

This script is designed for large S3 prefixes where each chunk folder should
contain up to N images (default: 50). It uses batched `rclone move` operations
with `--files-from` for better throughput than one-file-at-a-time moves.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Iterable


DEFAULT_EXTENSIONS = (
    ".png",
    ".jpg",
    ".jpeg",
    ".bmp",
    ".gif",
    ".tif",
    ".tiff",
    ".webp",
)


def run_cmd(cmd: list[str], quiet: bool = False) -> subprocess.CompletedProcess[str]:
    """Run shell command and return completed process."""
    kwargs = {"capture_output": True, "text": True}
    proc = subprocess.run(cmd, **kwargs)
    if proc.returncode != 0 and not quiet:
        message = (proc.stderr or proc.stdout or "").strip()
        print(f"Command failed ({proc.returncode}): {' '.join(cmd)}", file=sys.stderr)
        if message:
            print(message, file=sys.stderr)
    return proc


def normalize_remote(remote: str) -> str:
    return remote.rstrip("/")


def list_files(remote_root: str) -> list[str]:
    proc = run_cmd(["rclone", "lsf", remote_root, "--files-only", "--recursive"])
    if proc.returncode != 0:
        raise RuntimeError("Failed to list files from remote")
    return [line.strip() for line in proc.stdout.splitlines() if line.strip()]


def split_existing_and_remaining(
    files: Iterable[str], exts: set[str], chunk_prefix: str
) -> tuple[dict[int, int], list[str]]:
    chunk_re = re.compile(rf"^{re.escape(chunk_prefix)}-(\d{{4}})/(?:.+)$", re.IGNORECASE)
    chunk_counts: dict[int, int] = defaultdict(int)
    remaining: list[str] = []

    for rel in files:
        chunk_match = chunk_re.match(rel)
        if chunk_match:
            chunk_idx = int(chunk_match.group(1))
            chunk_counts[chunk_idx] += 1
            continue

        lower = rel.lower()
        dot = lower.rfind(".")
        ext = lower[dot:] if dot != -1 else ""
        if ext in exts:
            remaining.append(rel)

    remaining.sort()
    return chunk_counts, remaining


def compute_start(chunk_counts: dict[int, int], batch_size: int) -> tuple[int, int]:
    if not chunk_counts:
        return 1, 0
    last = max(chunk_counts)
    if chunk_counts[last] < batch_size:
        return last, chunk_counts[last]
    return last + 1, 0


def plan_moves(
    remaining: list[str],
    start_chunk: int,
    start_offset: int,
    batch_size: int,
    chunk_prefix: str,
) -> list[tuple[int, list[str]]]:
    plan: list[tuple[int, list[str]]] = []
    i = 0
    chunk = start_chunk
    offset = start_offset

    while i < len(remaining):
        capacity = batch_size - offset
        batch = remaining[i : i + capacity]
        plan.append((chunk, batch))
        i += len(batch)
        chunk += 1
        offset = 0

    return plan


def execute_plan(
    remote_root: str,
    plan: list[tuple[int, list[str]]],
    batch_size: int,
    chunk_prefix: str,
    dry_run: bool,
    transfers: int,
    checkers: int,
    upload_concurrency: int,
    verbose_every: int,
) -> tuple[int, int]:
    moved = 0
    failed = 0
    total = sum(len(batch) for _, batch in plan)
    processed = 0

    for chunk_idx, batch in plan:
        if not batch:
            continue

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as tmp:
            for rel in batch:
                tmp.write(f"{rel}\n")
            manifest_path = Path(tmp.name)

        dest = f"{remote_root}/{chunk_prefix}-{chunk_idx:04d}"
        cmd = [
            "rclone",
            "move",
            remote_root,
            dest,
            "--files-from",
            str(manifest_path),
            "--no-traverse",
            "--ignore-existing",
            "--transfers",
            str(transfers),
            "--checkers",
            str(checkers),
            "--s3-upload-concurrency",
            str(upload_concurrency),
            "--fast-list",
            "-q",
        ]
        if dry_run:
            cmd.append("--dry-run")

        proc = run_cmd(cmd, quiet=True)
        try:
            manifest_path.unlink(missing_ok=True)
        except OSError:
            pass

        if proc.returncode == 0:
            moved += len(batch)
        else:
            failed += len(batch)
            msg = (proc.stderr or proc.stdout or "").strip()
            print(
                f"FAIL {chunk_prefix}-{chunk_idx:04d}: rc={proc.returncode} "
                f"batch={len(batch)} msg={msg[:250]}",
                file=sys.stderr,
            )

        processed += len(batch)
        if processed % verbose_every == 0 or processed == total:
            print(
                f"PROGRESS processed={processed}/{total} moved={moved} "
                f"failed={failed} current_chunk={chunk_idx:04d}"
            )

    return moved, failed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Chunk S3 images into chunk-* folders using batched rclone move."
    )
    parser.add_argument(
        "--remote",
        required=True,
        help="rclone remote path, e.g. amharoc:amharoc/train-clean",
    )
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--chunk-prefix", default="chunk")
    parser.add_argument("--extensions", nargs="+", default=list(DEFAULT_EXTENSIONS))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--transfers", type=int, default=64)
    parser.add_argument("--checkers", type=int, default=64)
    parser.add_argument("--s3-upload-concurrency", type=int, default=32)
    parser.add_argument(
        "--verbose-every",
        type=int,
        default=1000,
        help="Print progress every N processed images.",
    )

    args = parser.parse_args()

    if args.batch_size <= 0:
        print("--batch-size must be > 0", file=sys.stderr)
        return 2

    remote_root = normalize_remote(args.remote)
    exts = {e.lower() if e.startswith(".") else f".{e.lower()}" for e in args.extensions}

    print(f"Mode: {'DRY RUN' if args.dry_run else 'EXECUTE'}")
    print(f"Remote: {remote_root}")
    print(f"Batch size: {args.batch_size}")
    print(f"Extensions: {', '.join(sorted(exts))}")

    try:
        files = list_files(remote_root)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    chunk_counts, remaining = split_existing_and_remaining(
        files=files,
        exts=exts,
        chunk_prefix=args.chunk_prefix,
    )
    start_chunk, start_offset = compute_start(chunk_counts, args.batch_size)
    plan = plan_moves(
        remaining=remaining,
        start_chunk=start_chunk,
        start_offset=start_offset,
        batch_size=args.batch_size,
        chunk_prefix=args.chunk_prefix,
    )

    print(f"Existing chunk folders: {len(chunk_counts)}")
    if chunk_counts:
        last = max(chunk_counts)
        print(f"Last chunk: {args.chunk_prefix}-{last:04d} ({chunk_counts[last]} files)")
    print(f"Remaining images to move: {len(remaining)}")
    print(f"Batches to run: {len(plan)}")

    if not remaining:
        print("Nothing to move.")
        return 0

    moved, failed = execute_plan(
        remote_root=remote_root,
        plan=plan,
        batch_size=args.batch_size,
        chunk_prefix=args.chunk_prefix,
        dry_run=args.dry_run,
        transfers=args.transfers,
        checkers=args.checkers,
        upload_concurrency=args.s3_upload_concurrency,
        verbose_every=args.verbose_every,
    )

    print("DONE")
    print(f"Processed: {len(remaining)}")
    print(f"Moved: {moved}")
    print(f"Failed: {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
