package org.who.fs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.who.util.ClassUtil;
import org.who.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public abstract class FileSystem {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSystem.class);
    private static final Map<String, Class<? extends FileSystem>> SERVICE_FILE_SYSTEMS = new HashMap<>();

    private static void loadFileSystems() {
        LOGGER.info("Loading filesystems");
        synchronized (FileSystem.class) {
            ServiceLoader<FileSystem> serviceLoader = ServiceLoader.load(FileSystem.class);
            Iterator<FileSystem> it = serviceLoader.iterator();
            while (it.hasNext()) {
                FileSystem fs = null;
                try {
                    fs = it.next();
                    SERVICE_FILE_SYSTEMS.put(fs.getScheme(), fs.getClass());
                    LOGGER.info("{} from {}", fs.getClass(), ClassUtil.findContainingJar(fs.getClass()));
                } catch (ServiceConfigurationError e) {
                    LOGGER.warn("Cannot load: {} from {}", fs, ClassUtil.findContainingJar(fs.getClass()));
                    LOGGER.info("Full exception loading: {}", fs, e);
                }
            }
        }
    }

    public static Class<? extends FileSystem> getFileSystemClass(String scheme) throws IOException {
        loadFileSystems();

        Class<? extends FileSystem> clazz = SERVICE_FILE_SYSTEMS.get(scheme);

        return clazz;
    }

    private static FileSystem createFileSystem(URI uri)
            throws IOException {
        LOGGER.info("create file system");
        Class<?> clazz = getFileSystemClass(uri.getScheme());
        FileSystem fs = (FileSystem) ReflectionUtils.newInstance(clazz);
        fs.initialize(uri);
        return fs;
    }

    public void initialize(URI name) throws IOException {
    }

    public String getScheme() {
        throw new UnsupportedOperationException("Not implemented by the " + getClass().getSimpleName() + " FileSystem implementation");
    }

    private static URI stringToUri(String pathString) throws IOException {
        // We can't use 'new URI(String)' directly. Since it doesn't do quoting
        // internally, the internal parser may fail or break the string at wrong
        // places. Use of multi-argument ctors will quote those chars for us,
        // but we need to do our own parsing and assembly.

        // parse uri components
        String scheme = null;
        String authority = null;
        int start = 0;

        // parse uri scheme, if any
        int colon = pathString.indexOf(':');
        int slash = pathString.indexOf('/');
        if (colon > 0 && (slash == colon + 1)) {
            // has a non zero-length scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start) &&
                (pathString.length() - start > 2)) {
            start += 2;
            int nextSlash = pathString.indexOf('/', start);
            int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start, authEnd);
            start = authEnd;
        }
        // uri path is the rest of the string. ? or # are not interpreted,
        // but any occurrence of them will be quoted by the URI ctor.
        String path = pathString.substring(start, pathString.length());

        // Construct the URI
        try {
            return new URI(scheme, authority, path, null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


    public static void main(String[] args) throws URISyntaxException, IOException {
        FileSystem fs;
        if ("file".equals(args[0]))
            fs = createFileSystem(stringToUri("file:///home/iss/dist"));
        else if ("hdfs".equals(args[0]))
            fs = createFileSystem(stringToUri("hdfs://localhost:9000"));
    }
}
