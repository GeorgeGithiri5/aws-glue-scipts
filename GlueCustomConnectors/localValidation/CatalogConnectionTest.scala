package test.scala

import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.schema.builders.SchemaBuilder
import com.amazonaws.services.glue.schema.{Schema, TypeCode}
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class CatalogConnectionTest extends FunSuite with BeforeAndAfterEach{
    val conf = new SparkConf().setAppName("LocalTest").setMaster("local")
    var spark: SparkConf = _
    var glueContext: GlueContext = _

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    override def beforeEach(): Unit = {
        spark = new SparkContext(conf)
        glueContext = new GlueContext(spark)
    }

    override def afterEach(): Unit = {
        super.afterEach()
        spark.stop()
    }

    test("test catalog connection intergration"){
        val database = "connector"
        val tableName = "testsalesforce"

        val optionsMap = Map(
            "query" -> "SELECT NumberOfEmployees, CreatedDate FROM Account WHERE",
            "partitionColumn" -> "RecordId__c",
            "lowerBound" -> "0",
            "lowerBound" -> "13",
            "numPartitions" -> "2",
            "connectionName" -> "connection"
        )

        val customSource = glueContext.getSource(
            connectionType = "custom.jdbc",
            connectionOptions = JsonOptions(optionsMap),
            transformationContext = "customSource"
        )
        val dyf = customSource.getDynamicFrame()

        var expectedSchema = new Schema(new SchemaBuilder()
            .beginStruct(),
            .atomicField("NumberOfEmployees", TypeCode.INT)
            .atomicField("CreatedDate", TypeCode.TIMESTAMP)
            .endStruct().build()
        )
        assert(dyf.schema === expectedSchema)
        val expectedRowCount = 13
        assert(dyf.count === expectedRowCount)
    }
}