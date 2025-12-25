from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

sys.path.append(str(ROOT))


from src.bootcamp_data.config import make_paths
from src.bootcamp_data.io import read_parquet,write_parquet
from src.bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from src.bootcamp_data.transforms import parse_datetime, add_time_parts, winsorize, add_outlier_flag
from src.bootcamp_data.joins import safe_left_join

paths =make_paths(ROOT)


# 1. loads orders_clean.parquet and users.parquet
orders = pd.read_parquet(paths.Processed / "orders_clean.parquet")
users = pd.read_parquet(paths.Processed / "users.parquet")

# 2. checks:
# required columns
# assert_unique_key(users, "user_id")
require_columns(orders, ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"],)
require_columns(users, ["user_id", "country", "signup_date"])
assert_non_empty(orders, "orders_clean")
assert_non_empty(users, "users")
assert_unique_key(users, "user_id")

# 3. parses created_at
# 4. adds time parts
orders_t = (
    orders.pipe(parse_datetime, col="created_at", utc=True)
    .pipe(add_time_parts, ts_col="created_at")
)

# 5. joins orders → users safely (validate="many_to_one")

joined = safe_left_join(orders_t, users, on="user_id", validate="many_to_one", suffixes=("", "_user"),)

if len(joined) != len(orders_t):
    raise AssertionError("Row count changed on left join (join explosion?)")

# 6. adds:
# amount_winsor
# optional amount._is_outlier
joined = joined.assign(amount_winsor=winsorize(joined["amount"]))
if add_outlier_flag is not None:
    joined = add_outlier_flag(joined, "amount", k=1.5)

# 7. writes data/processed/analytics_table.parquet
out_path = paths.Processed / "analytics_table.parquet"
out_path.parent.mkdir(parents=True, exist_ok=True)
write_parquet(joined,out_path)

# Checkpoint: script runs and writes the file.
check_df=read_parquet(paths.Processed/'analytics_table.parquet')
#print(check_df.head())
#print(check_df.info())

summary = (
    joined.groupby("country", dropna=False)
    .agg(n=("order_id","size"), revenue=("amount","sum"))
    .reset_index()
    .sort_values("revenue", ascending=False)
)
print(summary)
summary.to_csv(paths.reports/"revenue_by_country.csv", index=False)
