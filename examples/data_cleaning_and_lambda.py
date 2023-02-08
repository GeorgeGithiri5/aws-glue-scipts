import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

glueContext = GlueContext(SparkContext.getOrCreate())

# Data catalog: database and table name
db_name = "payments"
tbl_name = "medicare"

# S3 location for output
output_dir = "s3://glue-sample-target/output-dir/medicare_parquet"

# Read data into a DynamicFrame using the Data Catalog metadata
medicare_dyf = glueContext.create_dynamic_frame.from_catalog(database=db_name)

medicare_res = medicare_dyf.resolveChoice(specs = [('provider id', 'cast:long')])

# Remove erroneous records
medicare_df = medicare_res.toDF()
medicare_df = medicare_df.where("`provider id` is NOT NULL")

# Apply a lambda to remove the '$'
chop_f = udf(lambda x: x[1:], StringType())
medicare_df = medicare_df.withColumn("ACC", chop_f(medicare_df["average covered charges"])).withColumn("ATP", chop_f(medicare_df["average total payments"])).withColumn("AMP", chop_f(medicare_df["average medicare payments"]))

medicare_df = DynamicFrame.fromDF(medicare_df, glueContext, "nested")

medicare_nest = medicare_tmp.apply_mapping(
    [('drg definition', 'string', 'drg', 'string'),
     ('provider id', 'string', 'provider.id', 'long'),
     ('provider name', 'string', 'provider.name', 'string'),
     ('provider city', 'string', 'provider.city', 'string'),
     ('provider state', 'string', 'provider.state', 'string'),
     ('provider zip code', 'long', 'provider.zip', 'long')
     ])

glueContext.write_dynamic_frame.from_options(
    frame = medicare_nest,
    connection_type = "s3",
    connection_options = {"path": output_dir},
    format = "parquet"
)