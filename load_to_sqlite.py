"""Load the three Olist source CSVs required by the delivery analysis."""
from pathlib import Path
from contextlib import closing
import sqlite3
import pandas as pd

ROOT = Path(__file__).resolve().parent
FILES = {
    'olist_orders_dataset.csv': 'olist_orders',
    'olist_order_reviews_dataset.csv': 'olist_order_reviews',
    'olist_customers_dataset.csv': 'olist_customers',
}

def main():
    missing = [name for name in FILES if not (ROOT / 'Data' / name).is_file()]
    if missing:
        raise SystemExit('Missing source CSVs in Data/: ' + ', '.join(missing)
                         + '. Download the Olist dataset linked in README.md first.')
    with closing(sqlite3.connect(ROOT / 'olist.db')) as connection, connection:
        for filename, table in FILES.items():
            data = pd.read_csv(ROOT / 'Data' / filename)
            data.to_sql(table, connection, if_exists='replace', index=False)
            print(f'Loaded {table}: {len(data):,} rows')
    print('Created olist.db. Run notebooks/delivery_analysis.ipynb next.')

if __name__ == '__main__':
    main()
