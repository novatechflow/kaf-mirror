# kaf-mirror v1.2.0 Release Notes

Release date: 2026-01-19

## Highlights
- Mirror-first topic behavior with default same-name mapping and regex capture substitution.
- Dynamic regex topic discovery that auto-creates target topics with matching partitions and replication factor.
- Metrics now track consumed vs produced throughput and surface source/target stall incidents.

## Features
- Compliance report scheduling with configurable cadence.
- Multi-provider AI routing with endpoint normalization.
- Configurable database retention (1–30 days).
- Topic discovery interval configuration (`replication.topic_discovery_interval`).

## Fixes & UX
- UI logout now clears session tokens.
- `/login.html` route is served directly.
- Password reset no longer prints secrets to stdout; a password file is generated.
- Lag calculation includes partitions with no consumed records.

## Configuration changes
- New: `replication.topic_discovery_interval` (default `5m`, `0` disables discovery).
- Updated behavior: `mirror-cli users reset-password` writes the password to a file; use `--password-file` to control the path.

## Upgrade notes
- Review any regex mappings and prefer capture substitution where appropriate (e.g., `orders-(.*)` -> `backup-$1`).
- If you rely on password reset output, update automation to read the password file.
