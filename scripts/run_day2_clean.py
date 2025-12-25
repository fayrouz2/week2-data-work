from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

sys.path.append(str(ROOT))


from src.bootcamp_data.quality import require_columns,assert_non_empty, assert_unique_key, assert_in_range
from src.bootcamp_data.config import make_paths
from src.bootcamp_data.io import read_parquet,write_parquet,read_users_csv,read_orders_csv
from src.bootcamp_data.transforms import enforce_schema, missingness_report, add_missing_flags,normalize_text



# 1. loads raw CSVs (orders + users)
paths =make_paths(ROOT)

pd_orders=read_orders_csv(paths.raw/'orders.csv')
pd_users=read_users_csv(paths.raw/'users.csv')


# 2. runs basic checks (columns + non-empty)
require_columns(pd_orders,  ["order_id","user_id","amount","quantity","created_at","status"]) 
require_columns(pd_users,  ["user_id","country","signup_date"]) #add cols
assert_non_empty(pd_orders)
assert_non_empty(pd_users)
assert_unique_key(pd_orders, "order_id")
assert_unique_key(pd_users, "user_id")


# 3. enforces schema (from Day 1)
pd_orders=enforce_schema(pd_orders)

# 4. creates a missingness report and saves it to reports/
missingness=missingness_report(pd_orders)
write_parquet(missingness,ROOT/"reports"/"missingness_report.csv")

# 5. normalizes status into status_clean
pd_orders["status_clean"]=normalize_text(pd_orders["status"])

# 6. adds missing flags for amount and quantity
pd_orders=add_missing_flags(pd_orders,["amount","quantity"])

# 7. writes orders_clean.parquet
write_parquet(pd_orders,paths.Processed/"orders_clean.parquet")
write_parquet(pd_users, paths.Processed / "users.parquet")

# Checkpoint: script runs end-to-end and writes outputs.
clean_order_pq=read_parquet(paths.Processed/"orders_clean.parquet")
missingness_pq=read_parquet(ROOT/"reports"/"missingness_report.csv")
print(clean_order_pq.head())
print(missingness_pq.head())

#fail fast 
assert_in_range(clean_order_pq["amount"], lo=0,
name="amount")
assert_in_range(clean_order_pq["quantity"], lo=0,
name="quantity")


