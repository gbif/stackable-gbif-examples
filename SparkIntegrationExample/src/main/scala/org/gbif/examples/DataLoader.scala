package org.gbif.examples

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.Using

class DataLoader {
  private val sparkSessionName: String = "data-loader"
  private val sparkWarehouse = new File("/stackable/warehouse").getAbsolutePath
  private val logger = Logger("DataLoader")
  def load(table: String, location: String): Unit = {
    Using(SparkSession.builder()
      .appName(sparkSessionName)
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()){ session =>
      import session.sql
      logger.info("Creating external table if it does not exists")
      sql(
        f"""CREATE EXTERNAL TABLE IF NOT EXISTS '$table'
           | FOW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
           | STORED as INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
           | LOCATION '$location'
           | TBLPROPERTIES ('avro.schema.url'='/data/occurrence.avsc')""".stripMargin)
    }
  }
}
