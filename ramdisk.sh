#!/usr/bin/env bash
#
# Create or destroy a RAM disk for TaoTree tests.
#
# Usage:
#   ./ramdisk.sh up [SIZE_MB]   — create RAM disk (default: 256 MB)
#   ./ramdisk.sh down            — unmount and destroy RAM disk
#   ./ramdisk.sh status          — show RAM disk status
#
# Gradle auto-detects /Volumes/RAMDisk and uses it as java.io.tmpdir for tests.
# Linux: /dev/shm is tmpfs by default — no setup needed.

set -euo pipefail

MOUNT_POINT="/Volumes/RAMDisk"
DEFAULT_SIZE_MB=256

case "${1:-status}" in
  up)
    SIZE_MB="${2:-$DEFAULT_SIZE_MB}"
    SECTORS=$((SIZE_MB * 2048))

    if mount | grep -q "$MOUNT_POINT"; then
      echo "RAM disk already mounted at $MOUNT_POINT"
      df -h "$MOUNT_POINT"
      exit 0
    fi

    echo "Creating ${SIZE_MB} MB RAM disk..."
    DISK_DEV=$(hdiutil attach -nomount "ram://$SECTORS" | tr -d '[:space:]')
    diskutil erasevolume APFS RAMDisk "$DISK_DEV" > /dev/null
    echo "Mounted at $MOUNT_POINT ($SIZE_MB MB)"
    df -h "$MOUNT_POINT"
    ;;

  down)
    if ! mount | grep -q "$MOUNT_POINT"; then
      echo "No RAM disk mounted at $MOUNT_POINT"
      exit 0
    fi
    echo "Unmounting RAM disk..."
    hdiutil detach "$MOUNT_POINT" > /dev/null
    echo "Done."
    ;;

  status)
    if mount | grep -q "$MOUNT_POINT"; then
      echo "RAM disk is UP"
      df -h "$MOUNT_POINT"
    else
      echo "RAM disk is DOWN (not mounted)"
      echo "Create with: $0 up [SIZE_MB]"
    fi
    ;;

  *)
    echo "Usage: $0 {up [SIZE_MB]|down|status}"
    exit 1
    ;;
esac
