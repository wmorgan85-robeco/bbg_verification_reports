# Databricks notebook source
# DBTITLE 1,Bootstrap
# MAGIC %run "./utils"

# COMMAND ----------

# DBTITLE 1,Imports
from io import BytesIO, StringIO
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from pandas import read_excel, read_csv

# COMMAND ----------

# DBTITLE 1,Data Lake Client
credentials = {
    "tenant": "71dd74e2-a620-4a8e-9ac4-a19e1ff9ddff",
    "client_id": clientid,
    "client_secret": clientsecret,
    "storage_account_name": "rbqrrhippoprd",
    "file_system_name": "datascientist",
}


def get_lake_storage_path(file_system_name, storage_account_name, bulk_folder=""):
    return f"abfss://{file_system_name}@{storage_account_name}.dfs.core.windows.net/{bulk_folder}"


class DataLakeServiceClientExtension:
    def __init__(self, hippo_cred):
        account_url = f"https://{hippo_cred['storage_account_name']}.dfs.core.windows.net"
        cred = ClientSecretCredential(
            tenant_id=hippo_cred["tenant"],
            client_id=hippo_cred["client_id"],
            client_secret=hippo_cred["client_secret"],
        )
        service_client = DataLakeServiceClient(account_url=account_url, credential=cred)
        self.fs_client = service_client.get_file_system_client(file_system=hippo_cred["file_system_name"])

    def read_from_storage(self, dst):
        file_client = self.fs_client.get_file_client(dst)
        return file_client.download_file().readall()

    def read_dataframe_from_excel(self, dst):
        return read_excel(BytesIO(self.read_from_storage(dst)), engine="openpyxl")

    def read_dataframe_from_excel_sheet(self, dst, sheet_name):
        return read_excel(BytesIO(self.read_from_storage(dst)), engine="openpyxl", sheet_name=sheet_name)

    def read_dataframe_from_csv(self, dst):
        return read_csv(BytesIO(self.read_from_storage(dst)), sep="|")

    def write_to_storage(self, dst, bytesobj):
        file_client = self.fs_client.get_file_client(dst)
        file_client.upload_data(bytesobj, overwrite=True)

    def write_df_to_csv(self, df, dst):
        s_buf = StringIO()
        df.to_csv(s_buf, index=False)
        s_buf.flush()
        _bytes = BytesIO(s_buf.getvalue().encode("utf-8"))
        self.write_to_storage(dst, _bytes)


_client_data_science = DataLakeServiceClientExtension(credentials)