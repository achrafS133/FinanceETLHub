"""Azure Data Lake (Blob) + Azure SQL targets — Microsoft-aligned cloud path for FinanceHub."""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text

from config.settings import settings

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:  # pragma: no cover
    BlobServiceClient = None  # type: ignore


class AzureLoader:
    def __init__(self) -> None:
        self.container = settings.AZURE_STORAGE_CONTAINER or "finance-data"
        self.conn_str = settings.AZURE_STORAGE_CONNECTION_STRING or os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING", ""
        )
        self.sql_url = settings.AZURE_SQL_CONNECTION_STRING or os.getenv("AZURE_SQL_CONNECTION_STRING", "")

        self.blob_client: BlobServiceClient | None = None
        if BlobServiceClient and self.conn_str:
            try:
                self.blob_client = BlobServiceClient.from_connection_string(self.conn_str)
                logger.info("Azure Blob Storage client initialized (ADLS/Blob data lake).")
            except Exception as e:
                logger.warning(f"Azure Blob init failed: {e}")

        self.sql_engine = None
        if self.sql_url and settings.USE_AZURE:
            try:
                self.sql_engine = create_engine(self.sql_url)
                with self.sql_engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logger.info("Connected to Azure SQL data warehouse.")
            except Exception as e:
                logger.warning(f"Azure SQL connection failed: {e}")
                self.sql_engine = None
        elif not settings.USE_AZURE:
            logger.debug("USE_AZURE=false — Azure SQL load skipped (set USE_AZURE=true to enable).")

    def upload_to_blob(self, local_path: str | Path, blob_name: str) -> bool:
        """Land raw files in Azure Blob (medallion bronze / data lake)."""
        if self.blob_client is None:
            return False
        local_path = Path(local_path)
        if not local_path.is_file():
            logger.error(f"Local file not found: {local_path}")
            return False
        try:
            container_client = self.blob_client.get_container_client(self.container)
            if not container_client.exists():
                container_client.create_container()
            blob_client = container_client.get_blob_client(blob_name)
            with open(local_path, "rb") as data:
                blob_client.upload_blob(data, overwrite=True)
            logger.success(f"Uploaded {local_path} → abfs://{self.container}/{blob_name}")
            return True
        except Exception as e:
            logger.error(f"Azure Blob upload failed: {e}")
            return False

    def upload_table(self, df: pd.DataFrame, table_name: str, if_exists: str = "append") -> bool:
        if self.sql_engine is None or df.empty:
            return False
        try:
            df.to_sql(table_name, self.sql_engine, if_exists=if_exists, index=False, chunksize=5000)
            logger.success(f"Loaded {len(df)} rows → Azure SQL dbo.{table_name}")
            return True
        except Exception as e:
            logger.error(f"Azure SQL load failed for {table_name}: {e}")
            return False

    @staticmethod
    def _fact_frame(processed_df: pd.DataFrame) -> pd.DataFrame:
        facts = processed_df.copy()
        return pd.DataFrame(
            {
                "invoice_no": facts["InvoiceNo"],
                "invoice_date": facts["InvoiceDate"],
                "customer_key": facts["CustomerID"],
                "product_key": facts["StockCode"],
                "quantity": facts["Quantity"],
                "unit_price": facts["UnitPrice"],
                "total_gbp": facts["Total_GBP"],
                "total_usd": facts["Total_USD"],
                "total_eur": facts["Total_EUR"],
                "total_mad": facts["Total_MAD"],
                "is_fraud_suspect": facts.get("Is_Fraud_Suspect", False),
                "is_deep_fraud_suspect": facts.get("Is_Deep_Fraud_Suspect", False),
                "fraud_probability": facts.get("Fraud_Probability", 0.0),
            }
        )

    @staticmethod
    def _customer_frame(processed_df: pd.DataFrame, rfm_df: pd.DataFrame) -> pd.DataFrame:
        country_map = (
            processed_df[["CustomerID", "Country"]]
            .drop_duplicates()
            .set_index("CustomerID")["Country"]
            .to_dict()
        )
        return pd.DataFrame(
            {
                "customer_key": rfm_df["CustomerID"],
                "country": rfm_df["CustomerID"].map(country_map).fillna("Unknown"),
                "rfm_segment": rfm_df["Customer_Segment"],
                "rfm_score": rfm_df["RFM_Score"],
                "is_current": True,
                "valid_from": pd.Timestamp.now(),
            }
        )

    def load_star_schema(self, processed_df: pd.DataFrame, rfm_df: pd.DataFrame | None = None) -> None:
        """Mirror star schema to Azure SQL (parallel to local Postgres)."""
        if self.sql_engine is None:
            return
        self.upload_table(self._fact_frame(processed_df), "fact_sales", if_exists="append")
        if rfm_df is not None:
            self.upload_table(
                self._customer_frame(processed_df, rfm_df), "dim_customer", if_exists="replace"
            )