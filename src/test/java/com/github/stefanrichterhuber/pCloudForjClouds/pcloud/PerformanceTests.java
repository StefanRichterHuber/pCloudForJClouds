package com.github.stefanrichterhuber.pCloudForjClouds.pcloud;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudUtils;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;

@Ignore("For performance tests only")
public class PerformanceTests {
    private final static Logger LOGGER = LoggerFactory.getLogger(PerformanceTests.class);

    private static final int TIME_TO_WAIT = 750;

    private BlobStore blobStore;
    private String container;

    @SuppressWarnings("rawtypes")
    @Rule
    public GenericContainer<?> redis = new GenericContainer(DockerImageName.parse("redis:5.0.3-alpine"))
            .withExposedPorts(6379);

    @Before
    public void setup() throws InterruptedException {
        final String token = System.getenv("PCLOUD_TOKEN");
        Properties properties = new Properties();
        properties.setProperty(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING,
                String.format("redis://%s:%d", redis.getHost(), redis.getFirstMappedPort()));
        properties.setProperty(PCloudConstants.PROPERTY_BASEDIR, "/S3");
        properties.setProperty(PCloudConstants.PROPERTY_CLIENT_SECRET, token);
        properties.setProperty(PCloudConstants.PROPERTY_USERMETADATA_FOLDER, "/test-metadata");
        properties.setProperty(PCloudConstants.PROPERTY_SYNCHRONIZE_METADATA_INTERVAL_MIN, "-1");
        // Either api.pcloud.com or eapi.pcloud.com for European accounts
        properties.setProperty(Constants.PROPERTY_ENDPOINT, PCloudUtils.testForAPIEndpoint(token).orNull());

        BlobStoreContext context = ContextBuilder.newBuilder("pcloud").overrides(properties)
                .build(BlobStoreContext.class);

        this.blobStore = context.getBlobStore();
        this.container = UUID.randomUUID().toString();

        blobStore.createContainerInLocation(null, container);
        assertTrue(blobStore.containerExists(container));
        Thread.sleep(TIME_TO_WAIT);
    }

    @After
    public void close() {
        blobStore.deleteContainer(container);
    }

    /**
     * Creates a byte array with random test data
     * 
     * @param size Size of the byte array
     * @return Generated array
     */
    public static byte[] createTestData(int size) {
        byte[] content = new byte[size];
        for (int j = 0; j < size; j++) {
            content[j] = (byte) (Math.random() * 255.0);
        }
        return content;
    }

    @Test
    public void shouldTestPerformanceOfSmallFiles() {
        int TEST_RUNS = 100;
        int FILE_SIZE = 4_000;
        int TEST_CYCLES = 5;

        List<byte[]> payloads = IntStream.range(0, TEST_RUNS).mapToObj(i -> createTestData(FILE_SIZE))
                .collect(Collectors.toList());

        List<CompletableFuture<Void>> jobs = new ArrayList<>();

        // Warm up
        LOGGER.info("Start warmup");
        for (int i = 0; i < TEST_RUNS; i++) {

            int cnt = i;
            jobs.add(CompletableFuture.supplyAsync(() -> {
                String blobName = "f2/warmup-timeblob" + Integer.toString(cnt);
                Blob blob = this.blobStore.blobBuilder(blobName).payload(payloads.get(cnt)).build();
                this.blobStore.putBlob(container, blob);
                return null;
            }));
        }
        PCloudUtils.allOf(jobs).join();
        jobs.clear();
        LOGGER.info("Warmup done...");

        List<Long> results = new ArrayList<>();

        for (int r = 0; r < TEST_CYCLES; r++) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < TEST_RUNS; i++) {

                int cnt = i;
                jobs.add(CompletableFuture.supplyAsync(() -> {
                    String blobName = "f1/timeblob" + Integer.toString(cnt);
                    Blob blob = this.blobStore.blobBuilder(blobName).payload(payloads.get(cnt)).build();
                    this.blobStore.putBlob(container, blob);
                    return null;
                }));
            }
            PCloudUtils.allOf(jobs).join();

            long end = System.currentTimeMillis();
            jobs.clear();
            results.add(end - start);
        }
        results.forEach(r -> {
            LOGGER.info("Uploading {} files of size {} bytes took {} ms", TEST_RUNS, FILE_SIZE, r);
        });
        long sum = results.stream().mapToLong(i -> i.longValue()).sum();
        LOGGER.info("Each run costs {} ms", sum / TEST_CYCLES);
        LOGGER.info("Each upload costs {}ms", sum /TEST_CYCLES / TEST_RUNS);
    }
}
