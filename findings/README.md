# Findings

Start with [performance.md](performance.md): what the system handles, what limited it, and what each change bought.

The per-run write-ups in `runs/` are in the order the tests happened:

| | run | what it tested |
|---|---|---|
| 01 | [baseline, refresh 50 ms](runs/01-baseline-refresh-50ms.md) | random read load 100 → 500/s plus bursts; Redis saturated by the cache refresher |
| 02 | [refresh 500 ms](runs/02-refresh-500ms.md) | a slower refresher; cache entries expired and starved the API's threads |
| 03 | [live bucket top-K](runs/03-live-bucket-top-k.md) | the merge fix: 20 ms → 1.5 ms per merge, Redis 90 % → 25 % |
| 04 | [stress, altered config](runs/04-stress-altered-config.md), [one-CPU cap](runs/04b-stress-one-cpu-cap.md) | CPU caps and extra API workers. Not the normal setup, kept for the record |
| 05 | [write path replay](runs/05-write-path-replay.md) | 9,600 likes/s counted after a 6 h replay |
| 06 | [profile](runs/06-profile.md) ([flame graph](runs/06-profile-flame.svg)) | where the API's CPU goes, measured with py-spy |
| 07 | [uvicorn[standard] + orjson](runs/07-uvicorn-standard-orjson.md) | before/after on the normal setup: 1,000 → 1,200 req/s |
| 08 | [replay after sleep](runs/08-replay-after-sleep.md) | system state after the laptop slept |

`data/` has the small summary files behind the tables. Raw per-request data is not in the repository
(`loadtest/results/` is ignored); rerun the scripts in `loadtest/` to produce it.

Hardware for all runs: MacBook Air M1 (8 cores), 16 GB RAM, Docker VM with 6 vCPUs / 6.1 GB, everything
running at once.
