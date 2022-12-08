package org.gbif.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gbif.examples.store.HDFSStore;

import java.io.IOException;

public class HadoopIntegrationExample {
    private static final Logger LOGGER = LogManager.getLogger(HadoopIntegrationExample.class);
    public static void main(String[] args) {
        LOGGER.info("Hello world!");
        Configuration loaded = new Configuration();
        try {
            HDFSStore store = new HDFSStore(loaded.get("fs.defaultFS"));
            store.copy("/data/occurrence", "/data/temporary");
        } catch (IOException e) {
            LOGGER.error("Unable to create filesystem for HDFS.", e);
            System.exit(1);
        }
        System.exit(0);
    }
}