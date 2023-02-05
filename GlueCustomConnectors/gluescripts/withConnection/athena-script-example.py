from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)

options = {
    "tableName":"all_log_streams",
    "schemaName": "/aws-glue/jobs/output",
    "connectionName": "my-connection"
}

datasource = glueContext.create_dynamic_frame_from_option(
    connection_type = "custom.athena",
    connection_options = options,
    transformation_ctx = "datasource"
)
datasource.show()
job.commit()