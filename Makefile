S3_BUCKET  ?= $(shell grep S3_BUCKET .env | cut -d= -f2)
S3_PREFIX  ?= $(shell grep S3_PREFIX .env | cut -d= -f2)
DAYS       ?= 30
FILTER     ?= ""
OUTPUT_DIR := export/output

.PHONY: help sync-full sync export query prune test clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-12s %s\n", $$1, $$2}'

# --- Sync ---
sync-full: ## Full Gmail import — run once on first setup
	python3 sync/gmail_sync.py --full

sync: ## Incremental delta sync via Gmail history API
	python3 sync/gmail_sync.py --incremental

# --- Job Tracker ---
jobs: ## Job search pipeline dashboard — make jobs DAYS=30
	python3 agent/job_tracker.py --days $(DAYS)

jobs-md: ## Job pipeline as markdown — make jobs-md DAYS=60
	python3 agent/job_tracker.py --days $(DAYS) --md

# --- Export ---
export: ## Export filtered emails to .md files for AI agents
	mkdir -p $(OUTPUT_DIR)
	python3 export/to_markdown.py --days $(DAYS) --filter $(FILTER) --output $(OUTPUT_DIR)
	@echo "Exported to $(OUTPUT_DIR) — use: q chat \"@$(OUTPUT_DIR) ...\""

# --- Query ---
query: ## Ad-hoc DuckDB query — make query Q="fitch recruiter last 30 days"
	python3 -c "import duckdb; print(duckdb.sql(\"SELECT date, from_addr, subject FROM read_parquet('s3://$(S3_BUCKET)/$(S3_PREFIX)/**/*.parquet') WHERE body_text ILIKE '%$(Q)%' ORDER BY date DESC LIMIT 20\").df().to_markdown())"

# --- Maintenance ---
prune: ## Remove export/output .md files older than DAYS
	find $(OUTPUT_DIR) -name "*.md" -mtime +$(DAYS) -delete
	@echo "Pruned $(OUTPUT_DIR)"

# --- Test ---
test: ## Run pytest suite
	pytest tests/ -v

# --- Clean ---
clean: ## Remove export output
	rm -rf $(OUTPUT_DIR)/*.md
