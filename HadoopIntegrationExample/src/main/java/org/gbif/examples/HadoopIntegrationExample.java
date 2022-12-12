package org.gbif.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gbif.examples.store.HDFSStore;

import java.io.IOException;

public class HadoopIntegrationExample {
    private static final Logger LOGGER = LogManager.getLogger(HadoopIntegrationExample.class);
    public static void main(String[] args) {

        Configuration loaded = new Configuration();
        try {
            loaded.addResource(new Path("file:///app/conf/core-site.xml"));
            loaded.addResource(new Path("file:///app/conf/hdfs-site.xml"));
            HDFSStore store = new HDFSStore(loaded.get("fs.defaultFS"),loaded);
            store.copy("/data/occurrence", "/data/temporary");
        } catch (IOException e) {
            LOGGER.error("Something went wrong!", e);
            System.exit(1);
        }
        System.exit(0);
    }
}