package org.who.fs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Slf4j
public class HdfsFileUtil {
    public static void main(String[] args) throws URISyntaxException, IOException {
        FileSystem fileSystem = new DistributedFileSystem();
        fileSystem.initialize(new URI("hdfs", null, "//localhost:9000", null, null), new Configuration());

        log.info("success");
    }
}
