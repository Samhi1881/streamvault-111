.PHONY: install test infra-up infra-down produce stream train score versions bench plots demo-files clean

install:
	pip install -e ".[all]"

test:
	pytest -q

infra-up:
	docker compose up -d

infra-down:
	docker compose down

produce:            ## send 50k transactions (1% malformed) to Kafka
	python -m streamvault.ingestion.producer --n 50000 --rate 2000 --bad-rate 0.01

stream:             ## run the streaming pipeline (Ctrl-C to stop)
	python -m streamvault.streaming.pipeline --source kafka --policy adaptive

train:
	python -m streamvault.ml.train --n 200000 --out models

score:
	python -m streamvault.ml.score --model models/xgb_fraud.json

versions:
	python -m streamvault.storage.versioning --table silver

bench:
	python benchmarks/checkpoint_benchmark.py --sizes 100000 500000 1000000 --stages 12 --repeats 3

plots:
	python benchmarks/plot_results.py

demo-files:         ## no-Docker demo: JSONL files -> pipeline
	python -m streamvault.ingestion.producer --to-dir data/stream_in --n 20000 --bad-rate 0.01 --rate 0
	python -m streamvault.streaming.pipeline --source files --input-dir data/stream_in --run-seconds 60

clean:
	rm -rf data/lake data/stream_in data/raw models results spark-warehouse metastore_db derby.log
