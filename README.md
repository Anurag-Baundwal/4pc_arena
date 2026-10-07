# 4pc_arena

Tools for testing and tuning four-player teams chess (4PC) engines, such as stockfish_4pc:

| Tool | Purpose |
|---|---|
| `match.py` | Play matches between two engines: SPRTs, fixed-length matches, and NNUE training data |
| `cluster.py` | Spread one SPRT across several machines on a local network |
| `spsa.py` | Tune engine parameters with SPSA |
| `fens.txt` | 10,000 balanced 4PC opening positions |

Everything runs on Python 3.9+ with the standard library alone, so nothing needs to be installed. Engines must speak UCI with the 4PC extensions that stockfish_4pc implements.

Each opening is played twice, with the engines swapping teams (Red/Yellow vs Blue/Green). Pairs of games are scored as a pentanomial. SPRT bounds are in normalized Elo, while the running Elo printed during a match is ordinary (logistic) Elo.

## Running an SPRT

```
python match.py --engine1 path/to/new_engine --engine2 path/to/base_engine \
    --tc 10000 --inc 100 --threads1 1 --threads2 1 --hash1 128 --hash2 128 \
    --workers 8 --fens fens.txt \
    --sprt --sprt-elo0 0 --sprt-elo1 3 --pairs 10000 \
    --out match_results.jsonl --fresh
```

- `--tc` and `--inc` are in **milliseconds**, per player. `--nodes`, `--depth` or `--movetime` give fixed-limit games instead.
- `--workers` is the number of games played at once. On a laptop, leave a core or two free for the system.
- `--sprt` stops as soon as the test passes or fails. `--pairs` caps the test if it never decides. The defaults are `--sprt-alpha 0.05 --sprt-beta 0.05`.
- Engine 1 is the one being tested. All results are from its perspective.

Without `--sprt`, `match.py` plays exactly `--pairs` pairs (or `--games` games), printing the score and Elo as it goes.

### Openings

- `--fens fens.txt` plays each pair from a FEN, in a shuffled order fixed by `--seed`. `--fens-start N` starts N positions into that order. It's useful for splitting one schedule into disjoint parts.
- `--opening-plies N` adds N random moves to every opening, shared by both games of a pair. With `--opening-nodes`, those moves are picked from the engine's MultiPV search instead of uniformly. `--opening-weights 50,30,15,5` sets the odds of picking the best, second best, and so on. `--opening-max-score` rejects guided openings that come out too unbalanced.

```
python match.py --engine1 new_engine --engine2 base_engine --tc 10000 --inc 100 \
    --workers 8 --fens fens.txt --opening-plies 8 --opening-nodes 10000 \
    --opening-weights 50,30,15,5 --opening-max-score 40 \
    --sprt --sprt-elo0 0 --sprt-elo1 5 --pairs 10000 --out match_results.jsonl --fresh
```

### Output and resuming

- `--out FILE.jsonl` saves every game, with its moves and search info, as it finishes. It also writes `.summary.json`, `.schedule.jsonl` and `.meta.json` files alongside it.
- **Resuming:** run the same command again without `--fresh` to continue an interrupted match. `--fresh` deletes the previous results and starts over.
- `--pgn4 [PATH]` also saves the games as Chess.com-style PGN4.
- `--moves` prints every move's search data, and `--quiet` prints only the final result.

## Spreading an SPRT across machines

`cluster.py` runs one SPRT with a coordinator on one machine and workers on any number of machines, such as a laptop plus an Android tablet in Termux. Faster workers simply play more pairs. Every worker needs its own copy of both engines, built from the same commits.

On the main machine, start the coordinator, then a local worker:

```
python cluster.py server --e1 new_engine --e2 base_engine --tc 40000 --inc 400 \
    --fens fens.txt --sprt-elo0 0 --sprt-elo1 3 --pairs 10000 --out match_results.jsonl --fresh

python cluster.py worker --server http://127.0.0.1:8080 --e1 new_engine --e2 base_engine \
    --concurrency 8 --threads 1 --hash1 128 --hash2 128
```

On another machine, point a worker at the coordinator's local IP address:

```
python cluster.py worker --server http://192.168.1.7:8080 --e1 ../stockfish_4pc/new_engine \
    --e2 ../stockfish_4pc/base_engine --concurrency 4 --threads 1 --hash1 128 --hash2 128
```

- **Firewall:** the coordinator listens on port 8080. On Windows, allow it once from an administrator terminal, and set your Wi-Fi network to Private:
  `netsh advfirewall firewall add rule name="SPRT Cluster Port 8080" dir=in action=allow protocol=TCP localport=8080`
- **Stopping:** the coordinator stops every worker once the SPRT decides. `--no-early-stop` plays all `--pairs` anyway.
- **Lost workers:** if a worker disappears, its pairs are handed to someone else after `--lease-timeout` seconds. Raise it for long time controls.
- **Termux:** run `termux-wake-lock` first, or Android may pause the worker. Tablets without a fan throttle under full load, so 4–5 concurrent games often beats using every core.
- **Memory:** each worker uses about `concurrency × (hash1 + hash2)` MB.

For SPRTs open to friends' machines over the internet, see the [OpenBench fork](https://github.com/Anurag-Baundwal/OpenBench), which uses `match.py` as its match runner.

## Generating NNUE training data

`--nnue-data-seeds` generates one dataset per seed, using the same engine on both sides:

```
python match.py --engine1 stockfish_4pc --engine2 stockfish_4pc --fens fens.txt \
    --opening-plies 8 --opening-max-score 250 --timeout 60 --workers 8 \
    --nnue-data-seeds 31 32 33 34 35
```

- **Default volume:** each seed plays 50,000 independent games at 10,000 nodes per move. Openings get 12 random plies, picked by 5,000-node searches. Any of these can be overridden, e.g. `--games`, `--nodes`, `--opening-plies`.
- **Output files:** each seed writes `engine_seed_N.txt` with one `| FEN | CENTIPAWN | RESULT |` sample per position, and `engine_seed_N.jsonl` holding the games.
- **Resuming:** rerunning the same command resumes unfinished seeds, without duplicating samples.

For a single dataset with your own file names, use `--out games.jsonl --nnue-output samples.txt` instead.

## Tuning with SPSA

`spsa.py` tunes parameters that the engine exposes as UCI options. It plays pairs between two perturbed copies of the engine, Stockfish-style:

```
python spsa.py --engine stockfish_4pc_tune --params eval_params.csv --state tune_state.json \
    --iterations 10000 --pairs 4 --workers 8 --threads 1 --hash 64 --nodes 10000 \
    --fens fens.txt --progress-interval 10
```

The parameter file has one parameter per line, as `name,start,min,max,c_end,r_end`, and `#` starts a comment:

```
# c_end = (max - min) / 20, r_end = 0.002
PawnValue,100,50,150,5,0.002
```

`--state` saves progress after every iteration, so rerunning the same command resumes the tune.

## Checking engine builds

Before a test, run `./stockfish_4pc bench` on both binaries. A node count that matches the expected value confirms the build, and rules out stale object files.
