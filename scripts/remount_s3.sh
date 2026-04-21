#!/usr/bin/env bash
set -euo pipefail

REMOTE="${1:-amharoc:amharoc}"
# Override with S3_MOUNTPOINT env var, or default to a user-local path.
# This keeps the script portable across machines/users.
DEFAULT_MOUNTPOINT="${HOME}/mnt/ocr-dataset"
MOUNTPOINT="${2:-${S3_MOUNTPOINT:-${DEFAULT_MOUNTPOINT}}}"

echo "Remounting ${REMOTE} -> ${MOUNTPOINT}"

# Try graceful unmount first, then force unmount if busy.
if mount | awk '{print $3}' | grep -qx "${MOUNTPOINT}"; then
  echo "Unmounting existing mount at ${MOUNTPOINT}"
  if ! umount "${MOUNTPOINT}" 2>/dev/null; then
    diskutil unmount force "${MOUNTPOINT}" >/dev/null
  fi
fi

mkdir -p "${MOUNTPOINT}"

echo "Mounting with rclone..."
rclone mount "${REMOTE}" "${MOUNTPOINT}" \
  --vfs-cache-mode writes \
  --allow-non-empty \
  --daemon

echo "Mounted. Verifying..."
if mount | awk '{print $3}' | grep -qx "${MOUNTPOINT}"; then
  echo "Success: ${REMOTE} mounted on ${MOUNTPOINT}"
  ls "${MOUNTPOINT}"
else
  echo "Mount verification failed." >&2
  exit 1
fi
