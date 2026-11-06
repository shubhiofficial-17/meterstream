.PHONY: install test lint eval demo up down
install: ; pip install -e ".[dev]"
test: ; pytest -q
lint: ; ruff check . && ruff format --check .
eval: ; python -m eval.evaluate_detectors --meters 1000 --days 7
demo: ; meterstream demo        # everything in one process, in memory: http://localhost:8000
up: ; docker compose up --build -d && echo "Dashboard: http://localhost:8000  Prometheus: http://localhost:9090"
down: ; docker compose down -v
