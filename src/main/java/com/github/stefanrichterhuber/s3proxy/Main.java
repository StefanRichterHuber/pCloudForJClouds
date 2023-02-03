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
            "--basedir" }, description = "Folder in pCloud containing all containers", defaultValue = "/S3")
    private String baseDir = "/S3";

    @Option(names = { "-r",
            "--redis" }, description = "redis connect string e.g. redis://localhost:6379")
    private String redis;

    @Option(names = "--verbose", negatable = true, description = "Verbose output")
    boolean verbose;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        /*
         * Configure loglevel based von the verbose flag
         */
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory
                .getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        root.setLevel(verbose ? Level.DEBUG : Level.INFO);

        /*
         * Configure S3 Proxy
         */
        final Properties blobStoreProperties = new Properties();
        blobStoreProperties.setProperty(PCloudConstants.PROPERTY_BASEDIR, baseDir);
        blobStoreProperties.setProperty(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING, redis);

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
        } catch (Exception e) {
            System.err.println(e.getMessage());
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
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
            System.out.println("Stopped s3 proxy");
        }));

        System.out.println("Started s3 proxy at " + endpoint);

        while (true) {
            Thread.sleep(10000);
        }
    }

}
