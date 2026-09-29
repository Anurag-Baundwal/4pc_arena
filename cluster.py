#!/usr/bin/env python3
"""
Lightweight distributed SPRT coordinator and worker for match.py.
Uses only Python standard library.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

# Re-use engine management, SPRT math, and types from your existing match.py
import match


class Coordinator:
    def __init__(
        self,
        config: match.MatchConfig,
        pairs_total: int,
        seed: int,
        out_path: Path | None,
        schedule_path: Path | None,
        lease_timeout_sec: float = 300.0,
        fresh: bool = False,
    ) -> None:
        self.config = config
        self.pairs_total = pairs_total
        self.out_path = out_path
        self.lease_timeout = lease_timeout_sec
        self.lock = threading.Lock()

        # Handle --fresh cleanup
        if fresh:
            if out_path:
                out_path.unlink(missing_ok=True)
                match.sidecar(out_path, ".summary.json").unlink(missing_ok=True)
            if schedule_path:
                schedule_path.unlink(missing_ok=True)

        print("Generating/loading opening schedule...", flush=True)
        self.schedule = self._load_or_create_schedule(config, pairs_total, seed, schedule_path)

        self.next_index = 1
        self.in_flight: dict[int, float] = {}  # pair_idx -> assigned_timestamp
        self.completed_pairs: set[int] = set()

        self.summary = match.SummaryAccumulator(
            pairs_total * 2, paired=True, sprt=config.sprt
        )
        self.sprt = self.summary.sprt

        # Resume: Read existing games from JSONL if not --fresh
        if not fresh and out_path and out_path.is_file():
            self._resume_existing(out_path)

    def _load_or_create_schedule(
        self,
        config: match.MatchConfig,
        pairs: int,
        seed: int,
        path: Path | None,
    ) -> list[match.StartPosition]:
        # Try loading existing JSONL schedule line-by-line
        if path and path.is_file():
            loaded: list[match.StartPosition] = []
            try:
                with path.open(encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            item = json.loads(line)
                            loaded.append(
                                match.StartPosition(
                                    item.get("fen"),
                                    list(item.get("opening_moves", [])),
                                    str(item.get("source", "startpos")),
                                )
                            )
                if len(loaded) >= pairs:
                    return loaded[:pairs]
            except Exception:
                pass

        # Otherwise generate schedule cleanly in memory
        schedule = match.create_schedule(config, pairs, seed, None, False)
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as f:
                for idx, start in enumerate(schedule, 1):
                    f.write(
                        json.dumps(
                            {
                                "index": idx,
                                "fen": start.fen,
                                "opening_moves": start.opening_moves,
                                "source": start.source,
                            },
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
        return schedule

    def _resume_existing(self, path: Path) -> None:
        records: list[dict[str, Any]] = []
        pair_counts: dict[int, int] = {}
        with path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    records.append(rec)
                    p_idx = rec.get("pair") or rec.get("game")
                    if p_idx is not None:
                        p_idx = int(p_idx)
                        pair_counts[p_idx] = pair_counts.get(p_idx, 0) + 1
                except Exception:
                    continue

        # A pair is complete only if both games (Game 1 & Game 2) are present
        for p_idx, count in pair_counts.items():
            if count >= 2:
                self.completed_pairs.add(p_idx)

        pair_added_count: dict[int, int] = {}
        for rec in records:
            p_idx = rec.get("pair") or rec.get("game")
            if p_idx is not None:
                p_idx = int(p_idx)
                if p_idx in self.completed_pairs and pair_added_count.get(p_idx, 0) < 2:
                    self.summary.add(rec)
                    pair_added_count[p_idx] = pair_added_count.get(p_idx, 0) + 1

        # Fast-forward next_index to the first incomplete pair
        while self.next_index <= self.pairs_total and self.next_index in self.completed_pairs:
            self.next_index += 1

        if self.completed_pairs:
            print(
                f"\n[Resume] Successfully restored {len(self.completed_pairs)} pairs "
                f"({self.summary.games_completed} games).",
                flush=True,
            )
            print(f"[Resume] Next pair to play: Pair {self.next_index:04d}\n", flush=True)
            stats_snapshot = self.summary.summary()
            if self.config.sprt:
                match.print_sprt_report(stats_snapshot, self.config)
            else:
                match.print_summary(stats_snapshot, "Engine1", "Engine2")

            summary_path = match.sidecar(self.out_path, ".summary.json") if self.out_path else None
            if summary_path:
                match.atomic_json(summary_path, stats_snapshot)

    def is_finished(self) -> bool:
        if self.sprt and self.sprt.terminal:
            return True
        return len(self.completed_pairs) >= self.pairs_total

    def get_task(self) -> dict[str, Any]:
        with self.lock:
            if self.is_finished():
                state = self.sprt.state if self.sprt else "completed"
                return {"stop": True, "reason": state}

            now = time.monotonic()
            assigned_pair: int | None = None

            # 1. Check for expired leases (orphaned by disconnected/crashed devices)
            for pair_idx, assigned_at in list(self.in_flight.items()):
                if pair_idx in self.completed_pairs:
                    self.in_flight.pop(pair_idx, None)
                    continue
                if now - assigned_at > self.lease_timeout:
                    print(f"[*] Pair {pair_idx:04d} lease expired. Reassigning.", flush=True)
                    assigned_pair = pair_idx
                    self.in_flight[pair_idx] = now
                    break

            # 2. Pick next unassigned pair (skipping any already completed)
            while self.next_index <= self.pairs_total and self.next_index in self.completed_pairs:
                self.next_index += 1

            if assigned_pair is None and self.next_index <= self.pairs_total:
                assigned_pair = self.next_index
                self.next_index += 1
                self.in_flight[assigned_pair] = now

            if assigned_pair is None:
                # All pairs are leased out, waiting for active devices to finish
                return {"wait": True}

            start = self.schedule[assigned_pair - 1]
            return {
                "stop": False,
                "pair_index": assigned_pair,
                "fen": start.fen,
                "opening_moves": start.opening_moves,
                "source": start.source,
            }

    def submit_result(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            pair_idx = payload["pair_index"]
            self.in_flight.pop(pair_idx, None)

            if pair_idx in self.completed_pairs:
                return {"ok": True, "stop": self.is_finished()}

            self.completed_pairs.add(pair_idx)
            rec1 = payload["record1"]
            rec2 = payload["record2"]

            # Save records
            if self.out_path:
                match.append_jsonl(self.out_path, rec1)
                match.append_jsonl(self.out_path, rec2)

            # Update stats
            self.summary.add(rec1)
            self.summary.add(rec2)

            # Print live SPRT status
            stats_snapshot = self.summary.summary()
            if self.config.sprt:
                match.print_sprt_report(stats_snapshot, self.config)
            else:
                match.print_summary(stats_snapshot, "Engine1", "Engine2")

            summary_path = match.sidecar(self.out_path, ".summary.json") if self.out_path else None
            if summary_path:
                match.atomic_json(summary_path, stats_snapshot)

            return {"ok": True, "stop": self.is_finished()}


class ClusterServerHandler(BaseHTTPRequestHandler):
    coordinator: Coordinator

    def log_message(self, format: str, *args: Any) -> None:
        pass  # Silence default HTTP access logs

    def do_GET(self) -> None:
        if self.path == "/config":
            c = self.coordinator.config
            payload = {
                "limit_kind": c.limit_kind,
                "limit_value": c.limit_value,
                "base_time_ms": c.base_time_ms,
                "increment_ms": c.increment_ms,
                "timeout": c.timeout,
                "margin_ms": c.margin_ms,
                "max_plies": c.max_plies,
                "stop": self.coordinator.is_finished(),
            }
            self._send_json(payload)
        elif self.path.startswith("/task"):
            self._send_json(self.coordinator.get_task())
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        if self.path == "/result":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length).decode("utf-8"))
            resp = self.coordinator.submit_result(body)
            self._send_json(resp)
        else:
            self.send_error(404)

    def _send_json(self, data: dict[str, Any]) -> None:
        raw = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


def run_server(args: argparse.Namespace) -> None:
    engine1 = match.EngineConfig(Path(args.engine1).resolve(), {}, 128, 1)
    engine2 = match.EngineConfig(Path(args.engine2).resolve(), {}, 128, 1)
    sprt_cfg = match.SprtConfig(args.sprt_elo0, args.sprt_elo1, args.sprt_alpha, args.sprt_beta)

    config = match.MatchConfig(
        engine1=engine1,
        engine2=engine2,
        arbiter=None,
        limit_kind=args.limit_kind,
        limit_value=args.limit_value,
        base_time_ms=args.tc,
        increment_ms=args.inc,
        timeout=args.timeout,
        margin_ms=args.margin,
        max_plies=args.max_plies,
        opening_plies=args.opening_plies,
        fens=match.load_fens(args.fens),
        workers=1,
        show_moves=False,
        sprt=sprt_cfg,
    )

    out = Path(args.out).resolve() if args.out else None
    schedule_path = match.sidecar(out, ".schedule.jsonl") if out else None

    coordinator = Coordinator(
        config,
        args.pairs,
        args.seed,
        out,
        schedule_path,
        lease_timeout_sec=args.lease_timeout,
        fresh=args.fresh,
    )
    ClusterServerHandler.coordinator = coordinator

    server = ThreadingHTTPServer((args.host, args.port), ClusterServerHandler)
    print(f"\n[Coordinator] Running on http://{args.host}:{args.port}")
    print(f"[Coordinator] SPRT: [{args.sprt_elo0}, {args.sprt_elo1}] max {args.pairs} pairs.")
    print("Press Ctrl+C to shut down.\n", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down coordinator...", flush=True)
    finally:
        server.server_close()


def http_get(url: str) -> dict[str, Any]:
    req = Request(url, headers={"User-Agent": "ClusterWorker/1.0"})
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_post(url: str, data: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps(data).encode("utf-8")
    req = Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "User-Agent": "ClusterWorker/1.0"},
    )
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))

def format_pair_result(s1: float, s2: float) -> str:
    tags = {1.0: "Win", 0.5: "Draw", 0.0: "Loss"}
    total = s1 + s2
    if total == 2.0:
        verdict = "E1 Double Win (+2)"
    elif total == 1.5:
        verdict = "E1 Win (+1)"
    elif total == 1.0:
        verdict = "Tie (=0)"
    elif total == 0.5:
        verdict = "E1 Loss (-1)"
    else:
        verdict = "E1 Double Loss (-2)"
    return f"{verdict} [G1: {tags.get(s1, '?')}, G2: {tags.get(s2, '?')}]"


def worker_thread(
    worker_id: int,
    server_url: str,
    config: match.MatchConfig,
    stop_event: threading.Event,
) -> None:
    active_engines = match.ActiveEngines()
    reusable: match.WorkerEngines | None = None
    try:
        reusable = match.WorkerEngines(config, active_engines)
        while not stop_event.is_set():
            try:
                task_data = http_get(f"{server_url}/task")
            except (URLError, TimeoutError, OSError):
                time.sleep(2)
                continue

            if task_data.get("stop"):
                stop_event.set()
                break
            if task_data.get("wait"):
                time.sleep(2)
                continue

            pair_index = task_data["pair_index"]
            start = match.StartPosition(
                task_data["fen"],
                task_data["opening_moves"],
                task_data["source"],
            )

            # Play Game 1: Engine 1 on RY, Engine 2 on BG
            task1 = match.GameTask(pair_index, "ry", start, paired=True)
            rec1 = match.play_game(config, task1, stop_event, active_engines, reusable)

            # Play Game 2: Engine 1 on BG, Engine 2 on RY
            task2 = match.GameTask(pair_index, "bg", start, paired=True)
            rec2 = match.play_game(config, task2, stop_event, active_engines, reusable)

            # Post results back to coordinator
            try:
                result = http_post(
                    f"{server_url}/result",
                    {
                        "pair_index": pair_index,
                        "record1": rec1,
                        "record2": rec2,
                    },
                )
                
                s1 = rec1["engine1_score"]
                s2 = rec2["engine1_score"]
                print(f"[Worker {worker_id}] Pair {pair_index:04d}: {format_pair_result(s1, s2)}", flush=True)

                if result.get("stop"):
                    stop_event.set()
                    break
            except (URLError, TimeoutError, OSError) as e:
                print(f"[Worker {worker_id}] Warning submitting result: {e}", file=sys.stderr)
    finally:
        if reusable:
            reusable.close()
        active_engines.close_all()


def run_worker(args: argparse.Namespace) -> None:
    print(f"Connecting to coordinator at {args.server}...", flush=True)
    try:
        match_info = http_get(f"{args.server}/config")
    except Exception as e:
        print(f"Error connecting to server: {e}", file=sys.stderr)
        return

    if match_info.get("stop"):
        print("Server indicates the match is already completed.")
        return

    # Engine configs local to this device
    e1 = match.EngineConfig(Path(args.engine1).resolve(), {}, args.hash1, args.threads)
    e2 = match.EngineConfig(Path(args.engine2).resolve(), {}, args.hash2, args.threads)
    arb = (
        match.EngineConfig(Path(args.arbiter).resolve(), {}, 16, 1)
        if args.arbiter
        else None
    )

    config = match.MatchConfig(
        engine1=e1,
        engine2=e2,
        arbiter=arb,
        limit_kind=match_info["limit_kind"],
        limit_value=match_info["limit_value"],
        base_time_ms=match_info["base_time_ms"],
        increment_ms=match_info["increment_ms"],
        timeout=match_info["timeout"],
        margin_ms=match_info["margin_ms"],
        max_plies=match_info["max_plies"],
        opening_plies=0,
        fens=[],
        workers=args.concurrency,
        show_moves=False,
    )

    stop_event = threading.Event()
    threads: list[threading.Thread] = []

    print(f"Starting {args.concurrency} worker thread(s) on this device...", flush=True)
    for i in range(args.concurrency):
        t = threading.Thread(
            target=worker_thread,
            args=(i + 1, args.server.rstrip("/"), config, stop_event),
            daemon=True,
        )
        t.start()
        threads.append(t)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping worker...", flush=True)
        stop_event.set()

    for t in threads:
        t.join(timeout=2)
    print("Worker stopped cleanly.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Distributed SPRT cluster runner for 4PC")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    # Server mode
    s_parser = subparsers.add_parser("server", help="Run the central coordinator")
    s_parser.add_argument("--host", default="0.0.0.0", help="Binding address (default: 0.0.0.0)")
    s_parser.add_argument("--port", type=int, default=8080, help="Port (default: 8080)")
    s_parser.add_argument("--engine1", "--e1", required=True, help="Engine 1 path (for opening generation)")
    s_parser.add_argument("--engine2", "--e2", required=True, help="Engine 2 path")
    s_parser.add_argument("--pairs", type=int, default=1000, help="Max pairs to play")
    s_parser.add_argument("--sprt-elo0", type=float, default=0.0)
    s_parser.add_argument("--sprt-elo1", type=float, default=5.0)
    s_parser.add_argument("--sprt-alpha", type=float, default=0.05)
    s_parser.add_argument("--sprt-beta", type=float, default=0.05)
    s_parser.add_argument("--nodes", type=int, default=10000)
    s_parser.add_argument("--tc", type=int, default=0)
    s_parser.add_argument("--inc", type=int, default=0)
    s_parser.add_argument("--timeout", type=float, default=30.0)
    s_parser.add_argument("--margin", type=int, default=50)
    s_parser.add_argument("--max-plies", type=int, default=1000)
    s_parser.add_argument("--opening-plies", type=int, default=0)
    s_parser.add_argument("--fens", default="")
    s_parser.add_argument("--seed", type=int, default=1)
    s_parser.add_argument("--out", default="cluster_results.jsonl")
    s_parser.add_argument("--fresh", action="store_true", help="Delete existing results and schedule to start fresh")
    s_parser.add_argument("--lease-timeout", type=float, default=900.0, help="Seconds before an unreturned pair is reassigned")

    # Worker mode
    w_parser = subparsers.add_parser("worker", help="Run a worker node")
    w_parser.add_argument("--server", required=True, help="Coordinator URL, e.g., http://192.168.1.50:8080")
    w_parser.add_argument("--engine1", "--e1", required=True, help="Local path to Engine 1 binary")
    w_parser.add_argument("--engine2", "--e2", required=True, help="Local path to Engine 2 binary")
    w_parser.add_argument("--arbiter", default="", help="Optional local arbiter engine")
    w_parser.add_argument("--concurrency", type=int, default=1, help="Concurrent games running on this device")
    w_parser.add_argument("--threads", type=int, default=1, help="Threads per engine instance")
    w_parser.add_argument("--hash1", type=int, default=64)
    w_parser.add_argument("--hash2", type=int, default=64)

    args = parser.parse_args()
    if args.mode == "server":
        args.limit_kind = "clock" if args.tc > 0 else "nodes"
        args.limit_value = args.tc if args.tc > 0 else args.nodes
        run_server(args)
    elif args.mode == "worker":
        run_worker(args)


if __name__ == "__main__":
    main()