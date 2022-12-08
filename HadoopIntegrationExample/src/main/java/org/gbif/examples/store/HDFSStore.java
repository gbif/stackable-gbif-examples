package org.gbif.examples.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;


public class HDFSStore {
    private final String hdfsUrl;

    private final FileSystem resolvedFileSystem;
    private final Configuration hdfsConfigurations;

    private static final Logger LOGGER = LogManager.getLogger(HDFSStore.class);

    public HDFSStore(String nameserverUrl) throws IOException {
        hdfsUrl = nameserverUrl;
        hdfsConfigurations = new Configuration(true);
        resolvedFileSystem = FileSystem.get(URI.create(hdfsUrl), hdfsConfigurations);
    }

    public HDFSStore(String nameserverUrl, Configuration userProvidedConfigurations) throws IOException {
        hdfsUrl = nameserverUrl;
        hdfsConfigurations = userProvidedConfigurations;
        resolvedFileSystem = FileSystem.get(URI.create(hdfsUrl), hdfsConfigurations);
    }

    public void copy(Path src, Path dest){
        try {
            if(!resolvedFileSystem.exists(dest)){
                resolvedFileSystem.mkdirs(dest);
            }
            resolvedFileSystem.copyFromLocalFile(src,dest);
        } catch (IOException e) {
            LOGGER.error("Unable to copy from {} to destination {}", src.toString(), dest.toString(), e);
        }
    }

    public void copy(String src, String dest){
        copy(new Path(src), new Path(dest));
    }
}
