"""Service entrypoints.

meterstream simulate --meters 1000 --speed 60     # publish simulated readings to Redis
meterstream worker                                 # consume, detect, store
meterstream api                                    # HTTP API + dashboard
meterstream demo                                   # everything in one process, in memory
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
import time

from meterstream.config import Settings, build_broker, build_store, load_solar_meters

log = logging.getLogger("meterstream")


def _stopper() -> callable:
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    return stop.is_set


def simulate(args, settings: Settings, broker=None, stop=None) -> None:
    from meterstream.simulator import FleetSimulator, SimConfig

    broker = broker or build_broker(settings)
    stop = stop or _stopper()
    cfg = SimConfig(
        meters=args.meters,
        interval_s=settings.interval_s,
        seed=args.seed,
        start_ts=int(time.time()) - 86_400 * args.backfill_days,
    )
    sim = FleetSimulator(cfg)
    solar = {m.profile.meter_id for m in sim.meters if m.profile.has_solar}
    broker.save_solar_registry(solar)
    if args.solar_out:
        with open(args.solar_out, "w") as f:
            f.writelines(m + "\n" for m in sorted(solar))
    # Each simulated interval is published as one batch; --speed compresses time (60 = 1 hour per minute).
    batch, current = [], cfg.start_ts
    for r in sim.run(10**9):
        if stop():
            break
        if r.ts != current:
            broker.publish(batch)
            batch, current = [], r.ts
            if r.ts > time.time() - settings.interval_s:  # caught up with "now": pace in real time
                time.sleep(settings.interval_s / args.speed)
        batch.append(r.to_dict())


def worker(args, settings: Settings, broker=None, store=None, stop=None) -> None:
    from meterstream.pipeline import Pipeline, run_worker

    broker = broker or build_broker(settings)
    pipe = Pipeline(store or build_store(settings), interval_s=settings.interval_s, solar_meters=set())

    def refresh_registry() -> None:
        # Registry file if configured, else the copy in the broker. Detectors hold a reference
        # to this set, so update it in place.
        latest = load_solar_meters(settings.solar_meters_file) or broker.solar_registry()
        if latest and latest != pipe.solar_meters:
            pipe.solar_meters.clear()
            pipe.solar_meters.update(latest)
            log.info("loaded %d meters with generation", len(latest))

    if getattr(args, "metrics_port", 0):
        from meterstream.metrics import serve_metrics

        serve_metrics(args.metrics_port)
    log.info("worker %s started", args.consumer)
    run_worker(broker, pipe, args.consumer, stop=stop or _stopper(), on_tick=refresh_registry)


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(prog="meterstream")
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("simulate")
    p.add_argument("--meters", type=int, default=1000)
    p.add_argument("--speed", type=float, default=60.0)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--backfill-days", type=int, default=2, help="history to publish before going live")
    p.add_argument("--solar-out", default="", help="write solar meter IDs here for the worker")
    p = sub.add_parser("worker")
    p.add_argument("--consumer", default=os.environ.get("HOSTNAME", "worker-1"))
    p.add_argument("--metrics-port", type=int, default=9100)
    p = sub.add_parser("api")
    p.add_argument("--port", type=int, default=8000)
    p = sub.add_parser("demo")
    p.add_argument("--meters", type=int, default=300)
    p.add_argument("--port", type=int, default=8000)
    args = ap.parse_args(argv)
    settings = Settings()

    if args.cmd == "simulate":
        simulate(args, settings)
    elif args.cmd == "worker":
        worker(args, settings)
    elif args.cmd == "api":
        import uvicorn

        uvicorn.run("meterstream.api:app", host="0.0.0.0", port=args.port)
    elif args.cmd == "demo":
        import uvicorn

        from meterstream import api
        from meterstream.transport.memory import MemoryBroker

        broker, store, stop = MemoryBroker(), api.get_store(), threading.Event()
        sim_args = argparse.Namespace(meters=args.meters, speed=600.0, seed=42, backfill_days=2, solar_out="")
        threading.Thread(target=simulate, args=(sim_args, settings, broker, stop.is_set), daemon=True).start()
        w_args = argparse.Namespace(consumer="demo")
        threading.Thread(target=worker, args=(w_args, settings, broker, store, stop.is_set), daemon=True).start()
        uvicorn.run(api.app, host="0.0.0.0", port=args.port)
        stop.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
