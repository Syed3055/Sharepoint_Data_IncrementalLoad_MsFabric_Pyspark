# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8d1f28be-eb11-4ffc-bfd0-f282e1f0b88e",
# META       "default_lakehouse_name": "Project_LH",
# META       "default_lakehouse_workspace_id": "35614f05-4341-426f-b74c-497770758d5e",
# META       "known_lakehouses": [
# META         {
# META           "id": "8d1f28be-eb11-4ffc-bfd0-f282e1f0b88e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import*
from pyspark.sql.types import *


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

customer_df = spark.sql("SELECT * FROM Project_LH.dbo.Customer_dt ")

display(customer_df)

product_df = spark.sql("SELECT * FROM Project_LH.dbo.Product_dt ")
display(product_df)

region_df = spark.sql("SELECT * FROM Project_LH.dbo.Regon_dt")
display(region_df)

sales_df = spark.sql("SELECT * FROM Project_LH.dbo.Sales_dt ")
display(sales_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customer_df.select([count(when(col(c).isNull(),c)).alias(c)for c in customer_df.columns]))
display(product_df.select([count(when(col(c).isNull(),c)).alias(c)for c in product_df.columns]))
display(region_df.select([count(when(col(c).isNull(),c)).alias(c)for c in region_df.columns]))
display(sales_df.select([count(when(col(c).isNull(),c)).alias(c)for c in sales_df.columns]))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
create table if not EXISTS meta_dt(
    table_name varchar(35),
    date_wt timestamp
)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
MERGE into meta_dt as t
using ( select 'sales' as table_name, '1900-01-01 00:00:00' as date_wt
UNION all SELECT 'product', '1900-01-01 00:00:00' 
UNION all select 'customer','1900-01-01 00:00:00'
union all SELECT 'region', '1900-01-01 00:00:00') s 
on t.table_name=s.table_name
when not matched then 
insert (table_name, date_wt)
values(s.table_name, s.date_wt)
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meta = spark.sql("""
select table_name from meta_dt
""")
for co in meta.collect():
    print(f"table: {co}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_config = [
    {
        "table_name":"customer",
        "df":customer_df,
        "path":"Tables/silver/customer_inc",
        "inc_col":"created_timestamp"
    },
    {
        "table_name":"product",
        "df":product_df,
        "path":"Tables/silver/product_inc",
        "inc_col":"created_timestamp"
    },
    {
        "table_name":"region",
        "df":region_df,
        "path":"Tables/silver/region_inc",
        "inc_col":"created_timestamp"
    },
    {
        "table_name":"sales",
        "df":sales_df,
        "path":"Tables/silver/sales_inc",
        "inc_col":"sales_timestamp"
    }

]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in table_config:
    table_name = table["table_name"]
    df = table["df"]
    path = table["path"]
    inc_col = table["inc_col"]
    meta_last_dt = spark.sql(f"""
    select date_wt from meta_dt where table_name = '{table_name}'
    """)
    c_date_wt = meta_last_dt.collect()[0][0].strftime('%Y-%m-%d %H:%M:%S')
    print(f"Table Name= {table_name} timestamp: {c_date_wt}")
    incremental_df = df.filter(f"{inc_col}>'{c_date_wt}'")
    if incremental_df.rdd.isEmpty():
        print(f"Table Name : {table_name} New Rows not found......")
        continue
    else:
        print(f"Table Name : {table_name} New Rows Found... Processing.....!")
    if table_name == 'customer':
        incremental_df= incremental_df.withColumn('email_name', split(col('email'),'@')[0])
    elif table_name == 'product':
        incremental_df = incremental_df.withColumn('product_name',upper(col('product_name')))
    elif table_name == 'region':
        incremental_df = incremental_df.withColumn('country',upper(col('country')))
    elif table_name == 'sales':
        incremental_df = incremental_df.withColumn('key', concat_ws('_',col('sales_id'),col('product_id'),col('customer_id'),col('region_id')))
    incremental_df.write.format('delta').mode('append').save(path)
    inc_date = incremental_df.agg(max(f"{inc_col}")).collect()[0][0].strftime("%Y-%m-%d %H:%M:%S")
    if inc_date is not None:
        spark.sql(f"""
        UPDATE meta_dt
        set date_wt ='{inc_date}'
        where table_name = '{table_name}'
        """)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

d = spark.sql(f"""
select date_wt from meta_dt where table_name='{table_name}'
""")
display(d)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
