import sys
from awsglue.utils import getResolvedOptions
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.job import GlueContext
from awsglue.job import Job
from pyspark import SparkConf

from awsglue.dynamicframe import DynamicFrame
from awsglue.gluetypes import Field, IntegerType, TimestampType, StructType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

options_dataSourceTest_jdbc = {
    "query":"select NumberOfEmployees, CreatedDate from Account",
    "className":"partner.jdbc.some.Driver",
    # Test Parameters
    "url":"jdbc:some:url:SecurityToken=abc;",
    "user":"user",
    "password":"password",
}

# ColumnPartitioningTest
# for JDBC connector only
options_columnPartitioningTest = {
    "query":"select NumberOfEmployees, CreatedDate from Account where ",
    "url":"jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    "secretId":"test-partner-driver",
    "className":"partner.jdbc.some.Driver",
    
    # test parameters
    "partitionColumn": "RecordId__c",
    "lowerBound":"0",
    "upperBound": "13",
    "numPartitions":"2",
}

# DBtableQueryTest
# for JDBC connector only
options_dbtableQueryTest = {
    "url": "jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    "secretId": "test-partner-driver",
    "className": "partner.jdbc.some.Driver",
    
    # test parameter
    "query":"select NumberOfEmployees, CreatedDate from Account"
}

# JDBCUrlTest - extra jdbc connections UseBulkAPI appended
# for JDBC connector only
options_JDBCUrlTest = {
    "query":"select NumberOfEmployees, CreatedDate from Account",
    "secretId": "test-partner-driver",
    "className":"partner.jdbc.some.Driver",
    
    "url":"jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};UseBulkApi=true",
}

options_secretsManagerTest = {
    "query":"select NumberOfEmployees, CreatedDate from Account",
    "url": "jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    "className":"partner.jdbc.some.Driver",
    "secretId":"test-partner-driver"
}

# FilterPredicateTest
# for JDBC connector only
options_filterPredicateTest = {
    "query":"select NumberOfEmployees, CreatedDate from Account where",
    "url":"jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken};",
    "secretId":"test-partner-driver",
    "className":"partner.jdbc.some.Driver",
    
    # test parameter
    "filterPredicate":"BillingState='CA'"
}

datasource0 = glueContext.create_dynamic_frame_from_options(
    connection_type = "marketplace.jdbc",
    connection_options = options_secretsManagerTest
)

expected_schema = StructType([
    Field("NumberOfEmployees", IntegerType()),
    Field("CreatedDate", TimestampType())
    ])
expected_count = 2

assert datasource0.schema() == expected_schema

print("Expected Schema: " + str(expected_schema.jsonValue()))
print("result schema: " + str(datasource0.schema().jsonValue()))

print("Result schema in tree structure: ")
datasource0.printSchema()

assert datasource0.count() == expected_count
print("Expected record count: " + str(expected_count))
print("Result record count: " + str(datasource0.count()))

datasource0.write(
    connection_type = "s3",
    connection_options = {"path":"s3://your/output/path/"},
    format = "json"
)

# Create a DynamicFrame on the Fly
jsonStrings = ['{"Name":"Andrew"}']
rdd = sc.parallelize(jsonStrings)
sql_df = spark.read.json(rdd)
df = DynamicFrame.fromDF(sql_df, glueContext, "new_dynamic_frame")

option_dataSinkTest = {
    "secretId": "test-partner-driver",
    "dbtable":"Account",
    "className": "partner.jdbc.some.Driver"
    "url":"jdbc:some:url:user=${user};Password=${Password};SecurityToken=${SecurityToken}"
}

glueContext.write_dynamic_frame.from_options(
    from=df,
    connection_type = "marketplace.jdbc",
    connection_options = options_dataSinkTest
)

job.commit()