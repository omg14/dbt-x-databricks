import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view (
    name = "trans_passengers"
)

def trans_passengers():
    df = spark.readStream.format("delta") \
        .load("/Volumes/workspace/bronze/bronzevolume/customers/data/")
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys = ["passenger_id"],
    sequence_by = col("passenger_id"),
    stored_as_scd_type = 1
)