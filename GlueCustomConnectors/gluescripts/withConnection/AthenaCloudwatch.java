object AthenaCloudWatch{
    def main(sysArgs: Array[String]){
        val conf = new SparkConf().setAppName("AthenaCloudwatch").setMaster("local")
        spark: SparkContext = new SparkContext(conf)
        val glueContext: GlueContext = new GlueContext(spark)
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)

        val optionsMap = Map(
            "tableName" -> "all_log_streams",
            "schemaName" -> "/aws-glue/jobs/output",
            "connectionName" -> "my-connection"
        )
        val customSource = glueContext.getSource(
            connectionType = "custom.athena",
            connectionOptions = JsonOptions(optionsMap)
        )
        val dyf = customSource.getDynamicFrame()
        dfy.printSchema()
        dyf.show()
    }
}