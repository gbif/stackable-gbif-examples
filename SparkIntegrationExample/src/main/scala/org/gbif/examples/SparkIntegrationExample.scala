package org.gbif.examples

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkIntegrationExample {
    def main(args: Array[String]): Unit = {
        val logger = Logger(this.getClass.toString)
        logger.info("Starting to process data")
        var url = "thrift://dummy-url"
        if (args.length > 0) {
            logger.info("Getting URL from args")
            url = args(0)
        }
        val session = SparkSession.builder()
          .appName("example")
          .config("hive.metastore.uris", url)
          .enableHiveSupport()
          .getOrCreate()

        session.sql("CREATE EXTERNAL TABLE IF NOT EXISTS alex.occurrence ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe' STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat' LOCATION 'hdfs://gbif-hdfs/data/temporary' TBLPROPERTIES ('avro.schema.url'='hdfs://gbif-hdfs/data/occurrence.avsc')")

        val pivotDF = session.sql("SELECT * FROM alex.occurrence")

        pivotDF.write.format("parquet").mode(SaveMode.Overwrite).saveAsTable("alex.occurrence_parquet")

        logger.info("Done processing data")

        session.stop()
    }
}
