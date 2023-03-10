import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import org.apache.spark.SparkContext

object JoinAndRelationalize {
    def main(sysArgs: Array[String]): Unit = {
        val sc: SparkContext = new SparkContext()
        val glueContext: GlueContext = new GlueContext(sc)

        // catalog: database and table names
        val dbName = "legistators"
        val tblPersons = "persons_json"
        val tblMemberShip = "Memberships_json"
        val tblOrganization = "organizations_json"

        // output s3 and temp directories
        val outputHistoryDir = "s3://glue-sample-target/output-dir/legislator_history"
        val outputLgSingleDir = "s3://glue-sample-target/output-dir/legislator_single"
        val outputLgPartitionedDir = "s3://glue-sample-target/output-dir/legislator_part"
        val redshiftTmpDir = "s3://glue-sample-target/temp-dir/"

        val persons: DynamicFrame = glueContext.getCatalogSource(database=dbName, tableName=tblPersons).getDynamicFrame()
        val memberships: DynamicFrame = glueContext.getCatalogSource(database=dbName, tableName=tblMemberShip).getDynamicFrame()
        var orgs: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName=tblOrganization).getDynamicFrame()

        // Keep the fields we need and rename some.
        orgs = orgs.dropFields(Seq("other_names", "identifiers")).renameField("id", "org_id").renameField("name", "org_name")

        // Join the frames to create history
        val personMemberships = persons.join(keys1 = Seq("id"), keys2 = Seq("person_id"), frame2 = memberships)
        val lHistory = orgs.join(keys1=Seq("org_id"), keys2 = Seq("organization_id"), frame2 = personMemberships)
            .dropFields(Seq("person_id", "org_id"))
        
        println("Writing to /legislator_history ...")
        lHistory.printSchema()

        glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions(Map("path"->outputHistoryDir)),
            format = "parquet", transformationContext = ""
        ).writeDynamicFrame(lHistory)

        println("Writing to /legislator_part, partitioned by Senate and House ...")

        glueContext.getSinkWithFormat(connectionType = "s3",
            options = JsonOptions(Map("path"->outputLgSingleDir, "partitionKeys"->List("org_name"))),
            format = "parquet", transformationContext = ""
        ).writeDynamicFrame(lHistory)

        // Write out relational databases
        println("Converting to flat tables ..")
        val frames: Seq[DynamicFrame] = lHistory.relationalize(rootTableName = "hist_root",
            stagingPath = redshiftTmpDir, JsonOptions.empty
        )

        frames.foreach {
            frame => 
            val options = JsonOptions(Map("dbtable" -> frame.getName(), "database" -> "dev"))
            glueContext.getJDBCSink(catalogConnection = "test-redshift-3",
            options = options,
            redshiftTmpDir = redshiftTmpDir
            ).writeDynamicFrame(frame)
        }
    }
}