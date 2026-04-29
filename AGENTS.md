# AGENTS.md

This repo is for the always-on trading execution runtime.

## What this repo is for
- real order submission
- open-order / fill monitoring
- intraday execution automation
- reconciliation against broker truth
- runtime safety controls

## What this repo is not for
- factor scoring
- EOD pipeline orchestration
- signal-decider reasoning
- dashboard-heavy presentation logic

## Working style
- protect service boundaries aggressively
- keep execution state transitions explicit
- favor restart-safe persistence over in-memory convenience
- optimize for operational clarity, not cleverness

## Expected bootstrap files
- `README.md`
- `ARCHITECTURE.md`
- `RULES.md`
- `TASKS.md`
- `docs/API.md`
- `.codex/config.toml`

## Codex guidance
When implementing work here:
1. read `README.md`, `ARCHITECTURE.md`, `RULES.md`, and `TASKS.md`
2. preserve the split between core(strategy/read) and exec(execution/runtime)
3. do not add broker-facing automation without durable state and audit records
4. prefer small vertical slices with tests over large speculative frameworks
