
# FinanceETLHub — Power BI (Microsoft)

## Retail star schema (Online Retail ETL)

### Data sources (pick one)

| Source | When to use |
|--------|-------------|
| **PostgreSQL** (Docker local) | Dev / demo |
| **Azure SQL** | Cloud-aligned production mirror (`USE_AZURE=true`) |
| **Microsoft Fabric** | Use the `macro-market-engine` semantic model for macro/markets (not retail facts) |

### PostgreSQL

- **Get Data** → PostgreSQL
- Server: `localhost`, Database: `finance_db`, User/Pass: `postgres` / `postgres`

### Azure SQL

- **Get Data** → Azure SQL database
- Server: `your-server.database.windows.net`, Database: `finance_dw`
- Import or DirectQuery (DirectQuery for large fact tables)

### Modeling

- **Fact:** `fact_sales`
- **Dims:** `dim_customer`, `dim_product`, `dim_date`
- Relationships: many-to-one from fact keys to dimensions

### Sample measures

1. **Total Revenue (GBP)** = `SUM(fact_sales[total_gbp])`
2. **AOV** = `[Total Revenue GBP] / DISTINCTCOUNT(fact_sales[invoice_no])`
3. **Active Customers** = `DISTINCTCOUNT(fact_sales[customer_key])`

### Macro / markets (Fabric)

Build the executive report from [`macro-market-engine/powerbi/BUILD_REPORT.md`](../../macro-market-engine/powerbi/BUILD_REPORT.md) on the **MacroMarket** Direct Lake model.