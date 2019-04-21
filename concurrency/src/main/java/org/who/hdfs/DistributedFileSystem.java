package org.who.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.who.fs.FileSystem;

import java.io.IOException;
import java.net.URI;

public class DistributedFileSystem extends FileSystem {
    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedFileSystem.class);

    public DistributedFileSystem() {
        LOGGER.info("{} construct", DistributedFileSystem.class.getCanonicalName());
    }

    public void initialize(URI name) throws IOException {
        LOGGER.info(getScheme());
    }

    public String getScheme() {
        return "hdfs";
    }
}
