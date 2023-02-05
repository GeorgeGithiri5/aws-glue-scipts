object Salesforcejdbc{
    def main(sysArgs: Array[String]){
        val conf = new SparkConf().setAppName("JDBCSalesforce").setMaster("local")
        val spark: SparkContext = new SparkContext(conf)
        val glueContext: GlueContext = new GlueContext(spark)
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)

        val optionsMap = Map(
            "dbTable" -> "Account",
            "partitionColumn" -> "RecordId__c",
            "lowerBound" -> "0",
            "upperBound" -> "123",
            "numPartitions" -> "6",
            "connectionName" -> "my-connection"
        )

        val customSource = glueContext.getSource(
            connectionType = "custom.jdbc",
            connectionOptions = JsonOptions(optionsMap)
            transformationContext = ""
        )
        val dyf = customSource.getDynamicFrame()
        dfy.printSchema()
        dfy.show()
    }
}