#!/bin/bash
# scripts/backup_db.sh — Daily PostgreSQL + session backup
#
# Usage:
#   bash scripts/backup_db.sh
#
# Set up as a daily cron job (runs at 3 AM):
#   crontab -e
#   0 3 * * * cd /path/to/tg_tracker && bash scripts/backup_db.sh >> logs/backup.log 2>&1
#
# Backups are kept for 14 days, then auto-deleted.
# Restore with:
#   gunzip backups/tg_tracker_2026-06-13.sql.gz
#   psql -U postgres -h localhost tg_tracker < backups/tg_tracker_2026-06-13.sql

set -e
cd "$(dirname "$0")/.."  # run from project root

mkdir -p backups

DATE=$(date +%Y-%m-%d_%H%M)
OUTFILE="backups/tg_tracker_${DATE}.sql.gz"
DB_NAME="tg_tracker"
DB_USER="postgres"

echo "[$(date)] Starting backup of database '$DB_NAME'..."

# Use docker exec if postgres is running in docker-compose, else local pg_dump
if docker ps --format '{{.Names}}' 2>/dev/null | grep -q postgres; then
    CONTAINER=$(docker ps --format '{{.Names}}' | grep postgres | head -1)
    docker exec "$CONTAINER" pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$OUTFILE"
else
    PGPASSWORD="${POSTGRES_PASSWORD:-securepassword}" pg_dump -U "$DB_USER" -h localhost -p 5432 "$DB_NAME" | gzip > "$OUTFILE"
fi

SIZE=$(du -h "$OUTFILE" | cut -f1)
echo "[$(date)] Database backup complete: $OUTFILE ($SIZE)"

find backups/ -name "tg_tracker_*.sql.gz" -mtime +14 -delete
echo "[$(date)] Cleaned up DB backups older than 14 days"

# Also back up Telethon session files — these can't be regenerated
# without re-authenticating every personal account from scratch.
SESSIONS_BACKUP="backups/sessions_${DATE}.tar.gz"
if [ -d "sessions" ] && [ -n "$(ls -A sessions 2>/dev/null)" ]; then
    tar -czf "$SESSIONS_BACKUP" sessions/
    echo "[$(date)] Sessions backed up: $SESSIONS_BACKUP"
    find backups/ -name "sessions_*.tar.gz" -mtime +14 -delete
fi

echo "[$(date)] Done."
