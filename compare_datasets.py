import os

import pandas as pd
from config.settings import settings

root = settings.BASE_DIR
path1 = root / "data" / "raw" / "online_retail.xlsx"
path2 = root / "online+retail" / "Online Retail.xlsx"

def check_file(path):
    if os.path.exists(path):
        df = pd.read_excel(path)
        return df['InvoiceDate'].min(), df['InvoiceDate'].max(), len(df)
    return None

print(f"File 1: {check_file(path1)}")
print(f"File 2: {check_file(path2)}")
