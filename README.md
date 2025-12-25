## Setup
python -m venv .venv
# activate venv
.venv\Scripts\Activate
pip install -r requirements.txt \
pip install -e .
## Run ETL
python scripts/run_etl.py
## Outputs
- data/processed/users.parquet
- data/processed/analytics_table.parquet
- data/processed/_run_meta.json
- reports/figures/*.png
## EDA
Open notebooks/eda.ipynb and run all cells.
