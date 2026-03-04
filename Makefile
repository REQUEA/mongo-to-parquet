.PHONY: help install test dry-run export verify cleanup format lint

help:
	@echo "MongoDB to Parquet Exporter - Available Commands"
	@echo "================================================="
	@echo ""
	@echo "Setup:"
	@echo "  make install      - Install dependencies using UV"
	@echo "  make install-pip  - Install dependencies using pip"
	@echo ""
	@echo "Export Operations:"
	@echo "  make build        - Build python package"
	@echo "  make dry-run      - Test export without writing files"
	@echo "  make export       - Run full export"
	@echo "  make verify       - Verify exported Parquet files"
	@echo "  make cleanup-dry  - Preview MongoDB cleanup (no deletion)"
	@echo "  make cleanup      - Delete exported data from MongoDB (DANGEROUS)"
	@echo ""
	@echo "Development:"
	@echo "  make format       - Format code with black"
	@echo "  make lint         - Lint code with ruff"
	@echo "  make test         - Run tests"
	@echo ""
	@echo "Monitoring:"
	@echo "  make logs         - Tail export logs (JSON formatted)"
	@echo "  make stats        - Show export statistics"

install:
	@echo "Installing dependencies with UV..."
	uv venv
	uv pip install -e .
	@echo "✓ Installation complete"
	@echo ""
	@echo "Activate virtual environment with:"
	@echo "  source .venv/bin/activate"

install-pip:
	@echo "Installing dependencies with pip..."
	python -m venv .venv
	. .venv/bin/activate && pip install -e .
	@echo "✓ Installation complete"

dry-run:
	@echo "Running export in dry-run mode..."
	python mongo_to_parquet.py --dry-run

export:
	@echo "Starting MongoDB export..."
	@echo "Logs will be written to: mongo_export.log"
	uv python mongo_to_parquet.py

verify:
	@echo "Verifying exported Parquet files..."
	python verify_export.py

cleanup-dry:
	@echo "Previewing MongoDB cleanup (no data will be deleted)..."
	python cleanup_mongodb.py --dry-run

cleanup:
	@echo "⚠️  WARNING: This will DELETE data from MongoDB!"
	@echo "Make sure you have verified exports and created backups."
	@echo ""
	python cleanup_mongodb.py --confirm

format:
	@echo "Formatting code with black..."
	black *.py

lint:
	@echo "Linting code with ruff..."
	ruff check *.py

test:
	@echo "Running tests..."
	pytest tests/

logs:
	@echo "Tailing export logs (Ctrl+C to stop)..."
	tail -f mongo_export.log | jq .

stats:
	@echo "Export Statistics:"
	@echo "=================="
	@if [ -f export_checkpoint.json ]; then \
		cat export_checkpoint.json | jq .; \
	else \
		echo "No checkpoint file found. Run export first."; \
	fi
	@echo ""
	@echo "Parquet Files:"
	@if [ -d parquet_output ]; then \
		find parquet_output -name "*.parquet" | wc -l | xargs echo "  Total files:"; \
		du -sh parquet_output | awk '{print "  Total size: " $$1}'; \
	else \
		echo "  No output directory found"; \
	fi

clean:
	@echo "Cleaning up temporary files..."
	rm -rf .venv
	rm -rf __pycache__
	rm -rf *.pyc
	rm -rf .pytest_cache
	rm -f mongo_export.log
	rm -f export_checkpoint.json
	@echo "✓ Cleanup complete"
