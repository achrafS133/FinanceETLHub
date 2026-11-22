# Azure & Microsoft deployment guide

FinanceHub is aligned to the **Microsoft data stack**:

| Workload | Microsoft service | Repo path |
|----------|-------------------|-----------|
| Retail ETL (this pipeline) | **Azure Blob** (data lake) + **Azure SQL** (star schema) | `src/warehouse/azure_loader.py`, `sql/azure/` |
| Macro / markets analytics | **Microsoft Fabric** Lakehouse + Direct Lake Power BI | [`macro-market-engine/`](../macro-market-engine/) |
| Orchestration (optional) | **Azure Data Factory** or Fabric **Data Pipelines** | mirror `dags/` logic in ADF |
| Dashboards | **Power BI** (local Postgres, Azure SQL, or Fabric semantic models) | `dashboards/powerbi/` |

## 1. Prerequisites

- Azure subscription (free tier works for SQL + Storage)
- [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) or Portal
- ODBC Driver 18 for SQL Server (for `pyodbc` from your machine)

## 2. Provision resources (Portal or CLI)

```bash
az group create -n rg-finance-etl -l westeurope
az storage account create -n stfinanceetlraw -g rg-finance-etl -l westeurope --sku Standard_LRS
az storage container create --account-name stfinanceetlraw -n finance-data
az sql server create -g rg-finance-etl -n sql-finance-etl -l westeurope -u sqladmin -p "<StrongPassword>!"
az sql db create -g rg-finance-etl -s sql-finance-etl -n finance_dw --service-objective S0
```

Allow your client IP on the SQL server firewall.

## 3. Configure `.env`

```env
USE_AZURE=true
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...
AZURE_STORAGE_CONTAINER=finance-data
AZURE_SQL_CONNECTION_STRING=mssql+pyodbc://sqladmin:<password>@sql-finance-etl.database.windows.net:1433/finance_dw?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no
```

## 4. Initialize Azure SQL schema

Run [`sql/azure/init_schema.sql`](../sql/azure/init_schema.sql) in Azure Data Studio or `sqlcmd`.

## 5. Run the pipeline

```bash
python main.py --step full
```

When credentials are set:

- Raw CSV is uploaded to **Blob** (`raw/online_retail.csv`).
- Star schema tables load to **Azure SQL** after local Postgres/SQLite.

## 6. Power BI & Fabric

- **Retail:** connect Power BI to Azure SQL or local Postgres (see `dashboards/powerbi/README.md`).
- **Macro engine:** Git-sync `macro-market-engine/fabric` into a Fabric workspace (see `macro-market-engine/docs/05_runbook.md`).

## 7. Security

- Prefer **Managed Identity** + Key Vault in production (store connection strings as secrets).
- Never commit `.env` or storage keys.