package org.gbif.examples

import com.typesafe.scalalogging.Logger

object SparkIntegrationExample {
    def main(args: Array[String]): Unit = {
        val logger = Logger(this.getClass.toString)
        logger.info("Starting to process data")
        val loader: DataLoader = new DataLoader()
        loader.load("occurrence","/data/occurrence")
        val pivoter: DataPivoter = new DataPivoter()
        pivoter.pivot("parquet_occurrence", "occurrence")
        logger.info("Done processing data")
    }
}
