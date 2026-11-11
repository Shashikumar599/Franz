# Load test report: full-20261110-210408-livetopk

| test | load | got/s | p50 | p99 | max | err % | HIT % | Redis CPU avg / max | API CPU avg / max | ZUNIONSTORE/s | µs per union | Redis PING p50 max | consumers CPU | bsky lag max | API 503s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| T1 | uniform 100/s | 100.0 | 3.4 | 75.9 | 355 | 0.00 | 99.7 | 22 / 31 | 26 / 48 | 87.8 | 1,233 | 3.37 | 3 | 332 | 0 |
| T2 | uniform 200/s | 200.0 | 2.5 | 76.9 | 377 | 0.00 | 100.0 | 22 / 28 | 32 / 42 | 90.9 | 1,200 | 0.75 | 3 | 322 | 0 |
| T3 | uniform 300/s | 300.0 | 2.8 | 824.4 | 1,910 | 0.00 | 99.9 | 26 / 34 | 49 / 86 | 83.5 | 1,526 | 4.21 | 5 | 328 | 0 |
| T4 | poisson 300/s | 299.9 | 3.4 | 626.9 | 1,037 | 0.00 | 100.0 | 26 / 35 | 49 / 113 | 83.4 | 1,502 | 0.81 | 6 | 314 | 0 |
| T5 | burst 300/s | 300.0 | 4.0 | 990.6 | 1,156 | 0.00 | 100.0 | 25 / 36 | 49 / 107 | 84.4 | 1,416 | 25.73 | 5 | 293 | 0 |
| T6 | uniform 500/s | 500.0 | 2.8 | 2,414.3 | 3,059 | 0.00 | 99.8 | 26 / 37 | 69 / 162 | 79.8 | 1,537 | 17.34 | 4 | 471 | 0 |
| T7 | burst 500/s | 449.0 | 407.8 | 5,538.8 | 6,347 | 10.20 | 100.0 | 27 / 44 | 80 / 156 | 70.1 | 1,941 | 112.74 | 8 | 426 | 0 |

Latency in ms from the scheduled send time. CPU % is of one core (100 % = one core busy).
Redis runs commands on one thread, so Redis CPU near 100 % means it is saturated.
