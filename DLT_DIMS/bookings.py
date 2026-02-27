import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table (
    name = "stage_bookings"
)

# load the table incrementally
def stage_bookings():
    df = spark.readStream.format("delta") \
               .load("/Volumes/workspace/bronze/bronzevolume/bookings/data/")
    return df

@dlt.view(
    name = "trans_bookings"
)

def trans_bookings():
    df = spark.readStream.table("stage_bookings")
    df.withColumn("amount", col("amount").cast(DoubleType())) \
        .withColumn("modified_date", current_timestamp()) \
        .withColumn("booking_date", to_date(col("booking_date"))) \
        .drop("_rescued_data")
    return df

# rules for checking/validating data
rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)   # dlt.expect_all results in warn, fail, or drop

def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df