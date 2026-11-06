# Redis memory and controlled migration

The project Docker endpoint is `redis://127.0.0.1:6380/0`. Redis listens on 6379
inside the container. `REDIS_HOST_PORT` controls the loopback host mapping; keep
it consistent with `REDIS_URL` in ignored `angel_md_redis/.env`.

On 2 November 2026, `localhost:6379` reached an Ubuntu WSL Redis instead of the
project container. A healthy `docker exec md_redis redis-cli ping` was insufficient
to establish which server the workers used. New worker-set startup now compares
the configured Redis `run_id` against the project container and refuses a mismatch.
The launcher also passes the resolved URL to every child process.

## Memory controls

- The Docker image is Redis 8.0.5, matching the migrated source snapshot version.
  Do not load this snapshot into an older Redis version.
- `redis.conf` enables AOF, fsync every second, hourly snapshots after at least
  one change (`save 3600 1`), `maxmemory 4gb` and
  `noeviction`. This limit governs Redis allocations, not total process/VM RSS.
  Snapshot/AOF forks, fragmentation, Linux and other applications need headroom.
- Reaching the ceiling can reject writes. Monitor it; eviction policies can
  discard entire streams, including unarchived data, and are unsuitable here.
- This laptop uses `.env` overrides `ARCHIVE_MAX_BUFFER_MB=8`,
  `ARCHIVE_READ_COUNT=1000`, `REDIS_RETAIN_MINUTES=60`. Row-buffer limits exclude
  transient Redis responses, Python/Arrow objects and compression allocations.
- Instrument JSON is decoded incrementally and scoped to the configured NSE
  equities/NFO option underlyings. History bootstrap loads relevant equities only.
  The generic loader can still load the complete master. Option caches live
  outside pandas `attrs`, avoiding deep copies of cached DataFrames on slices.
- Candle-history reads use smaller pages while retaining their completeness
  conditions. Instrument coverage and multi-day candle history are preserved.
- `automation_common.ps1` defaults OpenBLAS/OMP/MKL/NumExpr native pools to one
  thread per worker, unless an explicit process environment override exists.
  Workers already run concurrently. An isolated import check on this laptop
  measured about 535 MiB private allocation with default pools versus 53 MiB
  with one thread. This is an import measurement, not total worker memory.

## Retention guarantees and limits

The controller runs `run_redis_retention.py` once per five minutes during an open
session. Reports are in `runtime/redis-retention.json` and date retention logs in
`logs/automation/`. The CLI can also run a bounded manual pass with project Python.

One atomic Lua operation per known stream takes the earliest boundary among the
age cutoff, every group's delivered checkpoint, and every group's oldest pending
entry. Approximate `XTRIM MINID` keeps the boundary entry and limits work per pass.
An archive group must exist. Stale pending entries or a group at zero deliberately
block removal; no groups are destroyed or reset. Candle and pivot histories are
excluded. The age setting must be at least 30 minutes, with 60 used locally.

Retention relies on fsynced local Parquet publication before archive ACKs. It does
not wait for Drive; local files remain protected by the uploader's independent
checksum verification and retention rules. Loss of the laptop's local disk before
upload remains a risk. Existing producer `MAXLEN` caps are separate and can still
trim lagging data; do not lower them without checking consumer progress.

## Controlled migration and rollback

1. Disable scheduled recovery. Use `stop_all.ps1` to stop ingestion/analytics and
   drain archivers. Require a fresh drain marker and archive lag/pending zero for
   all streams. A timeout means keep Redis and archivers alive for recovery.
2. Save/copy the source snapshot, record its SHA-256, stream lengths, boundary
   records and consumer checkpoints. Back up the destination volume and private
   `.env`. Avoid copying secrets into transcripts or Git.
3. Use matching Redis versions and a separate loopback port. Temporarily disable
   destination AOF when loading `dump.rdb`, so an old AOF cannot override it.
4. Compare every stream length, first/last record and consumer checkpoint. Total
   key count may decrease as TTL keys expire. Enable AOF and wait for its initial
   rewrite to succeed, then restart with the normal AOF-enabled configuration.
5. Verify all stream data/checkpoints again after that actual restart. Stop the
   original Redis, change `.env`, and verify the Docker identity. Disable the old
   service's autostart if it served only this collector; retain its data.
6. Run safe retention, resume the scheduler, and verify fresh ticks, archive parts,
   errors, memory and independent Drive receipts. Report the collection gap;
   controlled migration does not backfill missing live ticks automatically.

Local rollback artifacts for this migration are in ignored
`angel_md_redis/runtime/redis-migration-20261102/`, including `source.rdb`,
`source-metadata.json`, `import-verification.json`, `initial-retention.json`,
`docker-data-before/`, and private `env-before`. The original Ubuntu Redis data
also remains on disk. Do not publish this directory or remove it prematurely.

Before resuming the old source, drain the new pipeline and assess new data written
since cutover; an automatic rollback would omit those new writes. Never flush
either server or use `docker compose down -v` as a recovery shortcut.

## Validation

Run the full suite from `angel_md_redis` with project Python. The opt-in Redis
tests use unique test keys in local DB15; set `TEST_REDIS_URL` to the intended
local test endpoint and `RUN_REDIS_INTEGRATION=1`. They do not flush databases.
See `DATA-COLLECTION-SETUP.md` for the rest of the deployment workflow.
