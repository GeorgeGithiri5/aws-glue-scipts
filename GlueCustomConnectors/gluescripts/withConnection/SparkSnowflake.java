object SparkSnowflake {
    def main(sysArgs: Array[String]){
        val conf = new SparkConf.setAppName("SparkSnowflake").setMaster("local")
        val spark: SparkContext = new SparkContext(conf)
        val glueContext: GlueContext = new GlueContext(spark)
        val rootLogger = Logger.getRootLogger()
        rootLogger.setLevel(Level.ERROR)

        val optionsMap = Map(
            "sfDatabase" -> "snowflake_sample_data",
            "sfSchema" -> "PUBLIC",
            "sfWarehouse" -> "WORKSHOP_123",
            "dbtable" -> "lineitem",
            "connectionName" -> "my-connection"
        )
        val customSource = glueContext.getSource(
            connectionType = "custom.spark",
            connectionOptions = JsonOptions(optionsMap),
            transformationContext = ""
        )
        val dyf = customSource.getDynamicFrame()
        dfy.printSchema()
        dfy.show()
    }
}