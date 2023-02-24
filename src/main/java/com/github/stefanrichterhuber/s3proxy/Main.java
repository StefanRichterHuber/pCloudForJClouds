package com.github.stefanrichterhuber.s3proxy;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.Callable;

import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.gaul.shaded.org.eclipse.jetty.util.component.AbstractLifeCycle;

import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;

import ch.qos.logback.classic.Level;
import picocli.CommandLine;
import picocli.CommandLine.Option;

public class Main implements Callable<Integer> {
    @Option(names = { "-e", "--endpoint" }, description = "S3 server endpoint", defaultValue = "127.0.0.1:8080")
    private String endpoint = "127.0.0.1:8080";

    @Option(names = { "-b",
            "--basedir" }, description = "Folder in pCloud containing all containers")
    private String baseDir;

    @Option(names = { "-m", "--metadatadir" }, description = "Folder in pCloud containing the metadata")
    private String metadataDir;

    @Option(names = { "-r",
            "--redis" }, description = "redis connect string e.g. redis://localhost:6379")
    private String redis;

    @Option(names = "-v", description = { "Specify multiple -v options to increase verbosity of log output.",
            "For example, `-v -v -v` or `-vvv`" })
    boolean[] verbosity;

    public static void main(final String[] args) {
        final int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    /**
     * Parses the configured log level.
     * 
     * @return {@link Level} found. Defaults to {@link Level#ERROR}
     */
    public Level parseLogLevel(Properties properties) {

        // Check environment variables
        final String value = properties.getProperty(PCloudConstants.PROPERTY_LOG_LEVEL);
        if (value != null) {
            return switch (value.toUpperCase()) {
                case "ERROR" -> Level.ERROR;
                case "WARN" -> Level.WARN;
                case "INFO" -> Level.INFO;
                case "DEBUG" -> Level.DEBUG;
                case "TRACE" -> Level.TRACE;
                default -> Level.ERROR;
            };
        } else {
            return Level.ERROR;
        }

    }

    /**
     * Reads the config from (in ascending priority)
     * <li>Default values
     * <li>Environment variables
     * <li>command line parameters
     * 
     * @return {@link Properties} read. Never null.
     */
    private Properties readConfig() {
        final Properties config = new Properties();
        // First: default values
        config.setProperty(PCloudConstants.PROPERTY_LOG_LEVEL, "ERROR");
        config.setProperty(PCloudConstants.PROPERTY_BASEDIR, "/S3");
        config.setProperty(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING, "redis://redis:6379");
        config.setProperty(PCloudConstants.PROPERTY_USERMETADATA_FOLDER, "/S3-metadata");
        config.put(PCloudConstants.PROPERTY_SANITIZE_METADATA_INTERVAL_MIN, "-1");
        config.put(PCloudConstants.PROPERTY_SYNCHRONIZE_METADATA_INTERVAL_MIN, "0");

        // Second: values from environment
        config.putAll(PCloudConstants.fromEnv());

        // Third: Values from command line parameters
        if (baseDir != null)
            config.setProperty(PCloudConstants.PROPERTY_BASEDIR, baseDir);
        if (redis != null)
            config.setProperty(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING, redis);
        if (metadataDir != null)
            config.setProperty(PCloudConstants.PROPERTY_USERMETADATA_FOLDER, baseDir);
        // Parse verbosity parameter from command line.
        if (verbosity != null && verbosity.length > 0) {
            if (verbosity.length > 0 && verbosity[0])
                config.setProperty(PCloudConstants.PROPERTY_LOG_LEVEL, "WARN");
            if (verbosity.length > 1 && verbosity[1])
                config.setProperty(PCloudConstants.PROPERTY_LOG_LEVEL, "INFO");
            if (verbosity.length > 2 && verbosity[2])
                config.setProperty(PCloudConstants.PROPERTY_LOG_LEVEL, "DEBUG");
            if (verbosity.length > 3 && verbosity[3])
                config.setProperty(PCloudConstants.PROPERTY_LOG_LEVEL, "TRACE");
        }
        return config;
    }

    @Override
    public Integer call() throws Exception {
        /*
         * Read configuration
         */
        final Properties blobStoreProperties = this.readConfig();

        /**
         * Configure logging
         */
        final ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(parseLogLevel(blobStoreProperties));

        final ch.qos.logback.classic.Logger LOGGER = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger(Main.class);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                    "Configuration determined from default values, environment variables and command-line parameters:");
            LOGGER.info("    S3 endpoint: {}", endpoint);
            for (final var e : blobStoreProperties.entrySet()) {
                LOGGER.info("    {} ({}): {}", e.getKey(),
                        e.getKey().toString().toUpperCase().replace('.', '_'),
                        e.getValue());
            }
        }

        /*
         * Configure S3 Proxy
         */
        final S3Proxy s3Proxy = S3Proxy.builder() //
                .endpoint(URI.create("http://" + endpoint)) //
                // identity and credentials here do not matter, cause we use a BlobStoreLocator
                .awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "access", "secret") //
                .build();
        s3Proxy.setBlobStoreLocator(new DynamicPCloudBlobStoreLocator(blobStoreProperties));

        /*
         * Start the S3 Proxy
         */
        try {
            s3Proxy.start();
        } catch (final Exception e) {
            LOGGER.error(e.getMessage());
            return 1;
        }

        /*
         * Wait for the server to startup
         */
        while (!s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
            Thread.sleep(1);
        }

        /*
         * Catch application shutdown like STRG+C
         */
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                s3Proxy.stop();
            } catch (final Exception e) {
                LOGGER.error(e.getMessage());
            }
            System.out.println("Stopped S3 proxy");
        }));

        System.out.println("Started S3 proxy at " + endpoint);

        while (true) {
            Thread.sleep(10000);
        }
    }
}
