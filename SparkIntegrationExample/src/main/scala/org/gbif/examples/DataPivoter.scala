package org.gbif.examples

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.util.Using
class DataPivoter{
  private val sparkSessionName: String = "data-pivoter"
  private val sparkWarehouse = new File("/stackable/warehouse").getAbsolutePath
  private val logger = Logger("DataPivoter")
  def pivot(table: String, sourceTable: String): Unit = {
    Using(SparkSession.builder()
      .appName(sparkSessionName)
      .config("spark.sql.warehouse.dir", sparkWarehouse)
      .enableHiveSupport()
      .getOrCreate()) { session =>
      import session.sql
      logger.info("Pivoting existing table into parquet format")
      sql(
        f"""CREATE TABLE '$table' STORED AS parquet AS
           | SELECT * from '$sourceTable'""".stripMargin)
    }
  }
}
