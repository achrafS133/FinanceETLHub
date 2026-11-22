import pandas as pd
from config.settings import settings

path = settings.BASE_DIR / "online+retail" / "Online Retail.xlsx"
xl = pd.ExcelFile(path)
print(xl.sheet_names)
