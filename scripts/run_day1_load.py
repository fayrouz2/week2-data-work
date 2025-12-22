from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]

sys.path.append(str(ROOT))

from src.bootcamp_data.config import make_paths
from src.bootcamp_data.io import read_parquet,write_parquet,read_users_csv,read_orders_csv
from src.bootcamp_data.transforms import enforce_schema

# load paths from make_paths(ROOT)
paths =make_paths(ROOT)

# read raw CSVs
pd_orders=read_orders_csv(paths.raw/'orders.csv')
pd_users=read_users_csv(paths.raw/'users.csv')

# apply enforce_schema to orders
pd_orders=enforce_schema(pd_orders)

# write Parquet outputs to data/processed/
write_parquet(pd_orders,paths.Processed/"orders.parquet")

#check
order_pq=read_parquet(paths.Processed/"orders.parquet")
print(order_pq.head())
print(order_pq.info())
