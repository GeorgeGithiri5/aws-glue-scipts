import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

redis_rdd = sc.fromRedisList("redis://redis_endpoint:6379/0", "key_name")

df = redis_rdd.toDF(["key", "value"])

df.write.parquet("s3://bucket_name/output", mode = "overwrite")

sc.stop()