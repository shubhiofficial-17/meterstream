"""Score the detectors against the simulator's ground-truth anomaly labels.

    python -m eval.evaluate_detectors                       # 200 meters x 7 days
    python -m eval.evaluate_detectors --meters 1000 --days 14 --report eval-report.json
    python -m eval.evaluate_detectors --min-f1 0.9          # CI quality gate

An incident is a true positive if it is on the same meter, of the same kind, and overlaps the
labelled anomaly window (with one interval of slack). Each label can be matched once.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

from meterstream.metrics import Metrics
from meterstream.models import AlertKind, Incident
from meterstream.pipeline import Pipeline
from meterstream.simulator import FleetSimulator, Label, SimConfig
from meterstream.storage.memory import MemoryStore


def simulate(meters: int, days: int, seed: int, batch: int = 2000) -> tuple[Pipeline, FleetSimulator, float, int]:
    cfg = SimConfig(meters=meters, seed=seed)
    sim = FleetSimulator(cfg)
    solar = {m.profile.meter_id for m in sim.meters if m.profile.has_solar}
    pipe = Pipeline(MemoryStore(), interval_s=cfg.interval_s, solar_meters=solar, metrics=Metrics())
    intervals = days * 86_400 // cfg.interval_s

    # Group by interval and process each interval before the clock moves on, like a live worker.
    per_interval: dict[int, list[dict]] = defaultdict(list)
    for r in sim.run(intervals):
        per_interval[r.ts].append(r.to_dict())
    started, count = time.perf_counter(), 0
    for i in range(intervals):
        ts = cfg.start_ts + i * cfg.interval_s
        msgs = per_interval.pop(ts, [])
        for j in range(0, len(msgs), batch):
            pipe.process(msgs[j : j + batch])
        count += len(msgs)
        pipe.tick(ts + cfg.interval_s)
    elapsed = time.perf_counter() - started
    return pipe, sim, elapsed, count


def match(incidents: list[Incident], labels: list[Label], interval_s: int):
    slack = interval_s
    by_key: dict[tuple[str, AlertKind], list[Label]] = defaultdict(list)
    for lb in labels:
        by_key[(lb.meter_id, lb.kind)].append(lb)
    used: set[int] = set()
    tp: list[tuple[Incident, Label]] = []
    fp: list[Incident] = []
    for inc in sorted(incidents, key=lambda i: i.start_ts):
        cands = [
            lb
            for lb in by_key.get((inc.meter_id, inc.kind), [])
            if id(lb) not in used and inc.start_ts <= lb.end_ts + slack and inc.end_ts >= lb.start_ts - slack
        ]
        if cands:
            lb = min(cands, key=lambda lb: abs(lb.start_ts - inc.start_ts))
            used.add(id(lb))
            tp.append((inc, lb))
        else:
            fp.append(inc)
    fn = [lb for lb in labels if id(lb) not in used]
    return tp, fp, fn


def score(tp, fp, fn) -> dict:
    out = {}
    for kind in AlertKind:
        t = [x for x in tp if x[1].kind == kind]
        f_p = [x for x in fp if x.kind == kind]
        f_n = [x for x in fn if x.kind == kind]
        p = len(t) / (len(t) + len(f_p)) if (t or f_p) else None
        r = len(t) / (len(t) + len(f_n)) if (t or f_n) else None
        f1 = 2 * p * r / (p + r) if p and r else (0.0 if p is not None and r is not None else None)
        out[kind.value] = {
            "labels": len(t) + len(f_n),
            "tp": len(t),
            "fp": len(f_p),
            "fn": len(f_n),
            "precision": p,
            "recall": r,
            "f1": f1,
        }
    all_p = len(tp) / (len(tp) + len(fp)) if tp or fp else 0
    all_r = len(tp) / (len(tp) + len(fn)) if tp or fn else 0
    out["overall"] = {
        "labels": len(tp) + len(fn),
        "tp": len(tp),
        "fp": len(fp),
        "fn": len(fn),
        "precision": all_p,
        "recall": all_r,
        "f1": 2 * all_p * all_r / (all_p + all_r) if all_p + all_r else 0,
    }
    return out


def _fmt(v):
    return "-" if v is None else f"{v:.3f}"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--meters", type=int, default=200)
    ap.add_argument("--days", type=int, default=7)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--min-f1", type=float, default=None)
    ap.add_argument("--report", type=Path, default=None)
    args = ap.parse_args(argv)

    pipe, sim, elapsed, count = simulate(args.meters, args.days, args.seed)
    incidents = list(pipe.store._incidents.values())
    tp, fp, fn = match(incidents, sim.labels, sim.cfg.interval_s)
    scores = score(tp, fp, fn)
    delays = defaultdict(list)
    for inc, lb in tp:
        delays[lb.kind.value].append(max(0, inc.detected_ts - lb.start_ts) / 60)

    print(
        f"{args.meters} meters x {args.days} days: {count:,} messages in {elapsed:.1f}s "
        f"({count / elapsed:,.0f} msg/s, single process, in-memory)\n"
    )
    print("| anomaly | labels | precision | recall | F1 | median detection delay |")
    print("|---|---|---|---|---|---|")
    for kind, s in scores.items():
        d = delays.get(kind)
        delay = f"{statistics.median(d):.0f} min" if d else "-"
        name = f"**{kind}**" if kind == "overall" else kind
        print(
            f"| {name} | {s['labels']} | {_fmt(s['precision'])} | {_fmt(s['recall'])} | {_fmt(s['f1'])} | "
            f"{delay if kind != 'overall' else ''} |"
        )

    if args.report:
        args.report.write_text(
            json.dumps(
                {"meters": args.meters, "days": args.days, "messages": count, "seconds": elapsed, "scores": scores},
                indent=2,
            )
        )
    if args.min_f1 is not None and scores["overall"]["f1"] < args.min_f1:
        print(f"\nFAIL: overall F1 {scores['overall']['f1']:.3f} < {args.min_f1}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
