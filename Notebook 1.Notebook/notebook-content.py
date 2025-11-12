# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5c8c217e-16f4-4499-b7c3-244fd38f8694",
# META       "default_lakehouse_name": "youtubers_LH",
# META       "default_lakehouse_workspace_id": "ec80c81a-c58e-4508-817c-17f6c253f83a",
# META       "known_lakehouses": [
# META         {
# META           "id": "5c8c217e-16f4-4499-b7c3-244fd38f8694"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
import pandas as pd
df = pd.("Tables/silver_video_details")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

from deltalake import DeltaTable
table_path = "abfss://workspace_name@onelake.dfs.fabric.microsoft.com/lakehouse_name.Lakehouse/Tables/table_name" # replace with your table abfss path
storage_options = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}
dt = DeltaTable(table_path, storage_options=storage_options)
limited_data = dt.to_pyarrow_dataset().head(1000).to_pandas()
display(limited_data)

# import duckdb
# table_name = dt.to_pyarrow_dataset()
# display(duckdb.sql(" SELECT * from table_name limit 1000 ").df())

# If the table is too large and might cause an Out of Memory (OOM) error,
# you can try using the code below. However, please note that delta_scan with default lakehouse is currently in preview.
# import duckdb
# display(duckdb.sql("select * from delta_scan('/lakehouse/default/Tables/bigdeltatable') limit 1000 ").df())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
