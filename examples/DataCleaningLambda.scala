import com.amazonaws.services.glue.types.StringNode
import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.glue.{DynamicRecord, GlueContext}
import org.apache.spark.SparkContext

object DataCleaningLambda {
    def main(args: Array[String]): Unit = {
        val sc: SparkContext = new SparkContext()
        val glueContext: GlueContext = new GlueContext(sc)

        // Data Catalog: database and table name
        val dbName = "payments"
        val tblName = "medicare"

        // S3 location for output
        val outputDir = "s3://glue-sample-target/output-dir/medicare_parquet"

        // Read data into DynamicFrame using the Data Catalog metadata
        val medicareDyf = glueContext.getCatalogSource(
            database=dbName,
            tableName = tblName
            ).getDynamicFrame()
        
        val medicareRes = medicareDyf.resolveChoice(specs = Seq(("provider id", "cast:long")))
        
        // Remove erroneous records where `provider id` is null
        val medicareFiltered = medicareRes.filter(_.getField("provider id").exists(_ != null))

        // Apply a lambda to remove the $ from prices so we can cast them.

        def chopFirst(col: String, newCol: String): DynamicRecord => DynamicRecord = { rec =>
            rec.getField(col) match {
                case Some(s: String) => rec.addField(newCol, StringNode(s.tail))
                case _=>
            }
            rec
        }

        val udf = chopFirst("average covered charges", "ACC") andThen
            chopFirst("average total payments", "ATP") andThen
            chopFirst("average medicare payments", "AMP")

        val medicareTmp = medicareFiltered.map(f=udf)

        medicareTmp.printSchema()
        println(s"count: ${medicareTmp.count} errors: ${medicareTmp.errorsCounts}")
        medicareTmp.errorsAsDynamicFrame.show()

        // Rename, cast amd nest with apply_mapping
        val medicareNest = medicareTmp.applyMapping(Seq(("drg definition", "string", "drg", "string"),
            ("provider id", "long", "provider.id", "long"),
            ("provider name", "string", "provider.name", "string"),
            ("provider city", "string", "provider.city", "string"),
            ("provider state", "string", "provider.state", "string"),
            ("provider zip code", "long", "provider.zip", "long"),
            ("hospital referral region description", "string", "rr", "string"),
            ("ACC", "string", "charges.covered", "double"),
            ("ATP", "string", "charges.total_pay", "double"),
            ("AMP", "string", "charges.medicare_pay", "double")
        ))
    glueContext.getSinkWithFormat(connectionType="s3", options=JsonOptions(Map("path"->outputDir)), format = "parquet").writeDynamicFrame(medicareNest)
    }
}