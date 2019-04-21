package org.who.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class LocalFileSystem extends FileSystem {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileSystem.class);

    public LocalFileSystem() {
        LOGGER.info("{} construct", LocalFileSystem.class.getCanonicalName());
    }

    public void initialize(URI name) throws IOException {
        LOGGER.info(getScheme());
    }

    public String getScheme() {
        return "file";
    }
}
