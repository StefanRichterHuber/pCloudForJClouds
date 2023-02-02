package com.github.stefanrichterhuber.pCloudForjClouds.pcloud;

import static com.google.common.io.BaseEncoding.base16;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.MutableBlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.io.Payloads;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudUtils;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.base.Charsets;

public class PCloudForJCloudTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(PCloudForJCloudTest.class);

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
        properties.setProperty(PCloudConstants.PROPERTY_USERMETADATA_FOLDER, "test-metadata");
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

    @Test
    public void shouldCreateAndDestroyContainer() throws InterruptedException {
        String container = UUID.randomUUID().toString();

        // Container should not exist at first
        assertFalse(blobStore.containerExists(container));

        // Container should be created
        blobStore.createContainerInLocation(null, container);
        Thread.sleep(TIME_TO_WAIT);
        // Container should be existing
        assertTrue(blobStore.containerExists(container));

        // Container should be deleted
        blobStore.deleteContainer(container);
        Thread.sleep(TIME_TO_WAIT);
        // Container should not exist in the end
        assertFalse(blobStore.containerExists(container));
    }

    @Test
    public void shouldDeleteAFullContainer() throws InterruptedException {
        String b1Name = UUID.randomUUID().toString();
        String b2Name = UUID.randomUUID().toString();
        String b3Name = UUID.randomUUID().toString();

        Blob b1 = blobStore.blobBuilder(b1Name)//
                .payload(b1Name).build();
        Blob b2 = blobStore.blobBuilder(b2Name)//
                .payload(b2Name).build();
        Blob b3 = blobStore.blobBuilder(b3Name)//
                .payload(b3Name).build();

        blobStore.putBlob(container, b1);
        blobStore.putBlob(container, b2);
        blobStore.putBlob(container, b3);

        Thread.sleep(TIME_TO_WAIT);

        assertTrue(blobStore.blobExists(container, b1Name));
        assertTrue(blobStore.blobExists(container, b2Name));
        assertTrue(blobStore.blobExists(container, b3Name));

    }

    @Test
    public void shouldClearAContainer() throws InterruptedException {
        String b1Name = UUID.randomUUID().toString();
        String b2Name = UUID.randomUUID().toString();
        String b3Name = UUID.randomUUID().toString();

        Blob b1 = blobStore.blobBuilder(b1Name)//
                .payload(b1Name).build();
        Blob b2 = blobStore.blobBuilder(b2Name)//
                .payload(b2Name).build();
        Blob b3 = blobStore.blobBuilder(b3Name)//
                .payload(b3Name).build();

        blobStore.putBlob(container, b1);
        blobStore.putBlob(container, b2);
        blobStore.putBlob(container, b3);

        Thread.sleep(TIME_TO_WAIT);

        assertTrue(blobStore.blobExists(container, b1Name));
        assertTrue(blobStore.blobExists(container, b2Name));
        assertTrue(blobStore.blobExists(container, b3Name));

        blobStore.clearContainer(container);
        Thread.sleep(TIME_TO_WAIT);
        assertTrue("Container should be still there", blobStore.containerExists(container));

        assertFalse(blobStore.blobExists(container, b1Name));
        assertFalse(blobStore.blobExists(container, b2Name));
        assertFalse(blobStore.blobExists(container, b3Name));

    }

    @Test
    public void shouldListContainerNames() throws InterruptedException {
        String c1 = UUID.randomUUID().toString();
        String c2 = UUID.randomUUID().toString();
        String c3 = UUID.randomUUID().toString();

        // Container should be created
        blobStore.createContainerInLocation(null, c1);
        blobStore.createContainerInLocation(null, c2);
        blobStore.createContainerInLocation(null, c3);

        assertTrue(blobStore.containerExists(c1));
        assertTrue(blobStore.containerExists(c2));
        assertTrue(blobStore.containerExists(c3));
        Thread.sleep(TIME_TO_WAIT);
        PageSet<? extends StorageMetadata> ps0 = blobStore.list();
        assertNotNull(ps0);

        List<String> names = ps0.stream().map(sm -> sm.getName()).collect(Collectors.toList());
        assertTrue(names.contains(c1));
        assertTrue(names.contains(c2));
        assertTrue(names.contains(c3));

        blobStore.deleteContainer(c1);
        blobStore.deleteContainer(c2);
        blobStore.deleteContainer(c3);
    }

    @Test
    public void shouldUploadAndDownloadContent() throws IOException, InterruptedException {
        String blobName = UUID.randomUUID().toString() + ".txt";
        String blobContent = UUID.randomUUID().toString();
        Map<String, String> md = new HashMap<>();
        md.put("Usermetadata1", "user meta data value1");
        // Blob should not exist
        assertFalse(blobStore.blobExists(container, blobName));

        // Upload content
        Blob blob = blobStore.blobBuilder(blobName)//
                .payload(blobContent) //
                .userMetadata(md) //
                .build();

        String etag = blobStore.putBlob(container, blob);
        assertNotNull("Should have an etag", etag);
        Thread.sleep(TIME_TO_WAIT);

        // Blob should exist
        assertTrue(blobStore.blobExists(container, blobName));

        // Download content
        Blob result = blobStore.getBlob(container, blobName, GetOptions.Builder.ifETagMatches(etag));
        assertNotNull(result);
        String resultContent = IOUtils.toString(result.getPayload().openStream(), StandardCharsets.UTF_8.name());
        assertEquals("Content should be equal", blobContent, resultContent);
        assertEquals(md, result.getMetadata().getUserMetadata());
        // Delete blob
        blobStore.removeBlob(container, blobName);
        Thread.sleep(TIME_TO_WAIT);

        // Blob should be deleted
        assertFalse(blobStore.blobExists(container, blobName));
        assertNull(blobStore.getBlob(container, blobName));

    }

    @Test
    public void shouldCopyBlob() throws InterruptedException, IOException {

        String sourceBlobName = UUID.randomUUID().toString() + ".txt";
        Map<String, String> md = new HashMap<>();
        md.put("Usermetadata1", "user meta data value1");
        String blobContent = UUID.randomUUID().toString();

        String targetBlobName = UUID.randomUUID().toString() + ".txt";

        // Upload content
        Blob blob = blobStore.blobBuilder(sourceBlobName)//
                .payload(blobContent) //
                .userMetadata(md)
                .build();
        String etag = blobStore.putBlob(container, blob);
        assertNotNull("Should have an etag", etag);
        Thread.sleep(TIME_TO_WAIT);

        // Blob should exist
        assertTrue(blobStore.blobExists(container, sourceBlobName));

        // now copy blob and rename it
        String targetEtag = blobStore.copyBlob(container, sourceBlobName, container, targetBlobName,
                CopyOptions.builder().ifMatch(etag).build());
        Thread.sleep(TIME_TO_WAIT);
        // TargetBlob should exist
        assertTrue(blobStore.blobExists(container, targetBlobName));

        // Content should by the same

        Blob result = blobStore.getBlob(container, targetBlobName, GetOptions.Builder.ifETagMatches(targetEtag));
        assertNotNull(result);
        String resultContent = IOUtils.toString(result.getPayload().openStream(), StandardCharsets.UTF_8.name());
        assertEquals("Content should be equal", blobContent, resultContent);
        assertEquals(md, result.getMetadata().getUserMetadata());

        // Delete blobs
        blobStore.removeBlob(container, sourceBlobName);
        blobStore.removeBlob(container, targetBlobName);
    }

    @Test
    public void shouldSupportMultiPartUpload() throws InterruptedException, IOException {
        List<String> content = Arrays.asList("O rose, thou art sick!\r\n", "The invisible worm,\r\n",
                "That flies in the night,\r\n", "In the howling storm.\r\n", "Has found out thy bed\r\n",
                "Of crimson joy,\r\n", "And his dark secret love\r\n", "Does thy life destroy.");
        String mergedContent = content.stream().collect(Collectors.joining());
        String blobName = UUID.randomUUID().toString() + ".txt";
        Map<String, String> md = new HashMap<>();
        md.put("Usermetadata1", "user meta data value1");

        LOGGER.info("Uploading to blob {} in container {}", blobName, container);

        MutableBlobMetadata metaData = blobStore.blobBuilder(blobName).payload(mergedContent)
                .contentLength(mergedContent.getBytes(Charsets.UTF_8).length).userMetadata(md).build().getMetadata();
        MultipartUpload multipartUpload = blobStore.initiateMultipartUpload(container, metaData, null);
        assertNotNull(multipartUpload);

        for (int i = 0; i < content.size(); i++) {
            blobStore.uploadMultipartPart(multipartUpload, i + 1, Payloads.newStringPayload(content.get(i)));
            Thread.sleep(100);
        }
        String etag = blobStore.completeMultipartUpload(multipartUpload, null);

        // Get content and check if it matches
        Thread.sleep(TIME_TO_WAIT);
        Blob blob = blobStore.getBlob(container, blobName);

        assertEquals(etag, blob.getMetadata().getETag());
        String resultContent = IOUtils.toString(blob.getPayload().openStream(), StandardCharsets.UTF_8.name());
        assertEquals(mergedContent, resultContent);

        Map<String, String> mdr = blob.getMetadata().getUserMetadata();
        assertEquals(md, mdr);

        assertEquals(etag, base16().lowerCase()
                .encode(blob.getMetadata().getContentMetadata().getContentMD5AsHashCode().asBytes()));

    }

    /**
     * Test multipart upload, but not all parts appear in order
     * 
     * @throws InterruptedException
     * @throws IOException
     */
    @Test

    public void shouldSupportMultiPartUploadWithRandomOrder() throws InterruptedException, IOException {
        List<String> content = Arrays.asList("O rose, thou art sick!\r\n", "The invisible worm,\r\n",
                "That flies in the night,\r\n", "In the howling storm.\r\n", "Has found out thy bed\r\n",
                "Of crimson joy,\r\n", "And his dark secret love\r\n", "Does thy life destroy.");
        String mergedContent = content.stream().collect(Collectors.joining());
        long contentLength = mergedContent.getBytes(Charsets.UTF_8).length;
        String blobName = UUID.randomUUID().toString() + ".txt";

        LOGGER.info("Uploading to blob {} in container {}", blobName, container);

        MutableBlobMetadata metaData = blobStore.blobBuilder(blobName).payload(mergedContent)
                .contentLength(contentLength).build().getMetadata();
        MultipartUpload multipartUpload = blobStore.initiateMultipartUpload(container, metaData, null);
        assertNotNull(multipartUpload);

        int[] order = { 0, 2, 4, 7, 6, 1, 3, 5 };
        for (int o = 0; o < order.length; o++) {
            int i = order[o];
            blobStore.uploadMultipartPart(multipartUpload, i + 1, Payloads.newStringPayload(content.get(i)));
            Thread.sleep(1000);
        }

        String etag = blobStore.completeMultipartUpload(multipartUpload, null);

        // Get content and check if it matches
        Thread.sleep(TIME_TO_WAIT);
        Blob blob = blobStore.getBlob(container, blobName);
        assertEquals(etag, blob.getMetadata().getETag());
        String resultContent = IOUtils.toString(blob.getPayload().openStream(), StandardCharsets.UTF_8.name());
        assertEquals(mergedContent, resultContent);

    }

    @Test
    public void shouldListMetadataRecursivly() throws InterruptedException {

        LOGGER.info("Uploading to container {}", container);

        String blobName1 = UUID.randomUUID().toString() + ".txt";
        String blobName2 = "pre-" + UUID.randomUUID().toString() + ".txt";

        final String folder = "rnd/";
        String blobName3 = folder + UUID.randomUUID().toString() + ".txt";
        String blobName4 = folder + "pre-" + UUID.randomUUID().toString() + ".txt";

        for (String name : Arrays.asList(blobName1, blobName2, blobName3, blobName4)) {
            Blob blob = blobStore.blobBuilder(name)//
                    .payload(name) //
                    .build();

            blobStore.putBlob(container, blob);
        }

        /*
         * Should list only the files directly in the container
         */
        {
            PageSet<? extends StorageMetadata> pageSet = blobStore.list(container, ListContainerOptions.NONE);
            assertEquals(2, pageSet.size());
            List<String> names = pageSet.stream().map(sm -> sm.getName()).collect(Collectors.toList());
            assertTrue(names.containsAll(Arrays.asList(blobName1, blobName2)));
        }

        /*
         * Should list only the files directly in the container with the prefix
         */
        {
            PageSet<? extends StorageMetadata> pageSet = blobStore.list(container,
                    ListContainerOptions.Builder.prefix("pre-"));
            assertEquals(1, pageSet.size());
            List<String> names = pageSet.stream().map(sm -> sm.getName()).collect(Collectors.toList());
            assertTrue(names.containsAll(Arrays.asList(blobName2)));
        }

        /*
         * Should list all files since recursive
         */
        {
            PageSet<? extends StorageMetadata> pageSet = blobStore.list(container,
                    ListContainerOptions.Builder.recursive());
            assertEquals(4, pageSet.size());
            List<String> names = pageSet.stream().map(sm -> sm.getName()).collect(Collectors.toList());
            assertTrue(names.containsAll(Arrays.asList(blobName1, blobName2, blobName3, blobName4)));
        }

        /*
         * Should list all files within the directory
         */
        {
            PageSet<? extends StorageMetadata> pageSet = blobStore.list(container,
                    ListContainerOptions.Builder.recursive().prefix(folder));
            assertEquals(2, pageSet.size());
            List<String> names = pageSet.stream().map(sm -> sm.getName()).collect(Collectors.toList());
            assertTrue(names.containsAll(Arrays.asList(blobName3, blobName4)));
        }

    }

    @Test
    public void shouldListMetadata() throws InterruptedException {
        final int TEST_FILES = 10;

        LOGGER.info("Uploading to container {}", container);

        List<String> contents = new ArrayList<>();
        List<String> blobNames = new ArrayList<>();
        List<String> etags = new ArrayList<>();
        for (int i = 0; i < TEST_FILES; i++) {
            contents.add(UUID.randomUUID().toString());
            blobNames.add((i % 2 == 0 ? "pre-" : "") + UUID.randomUUID().toString() + ".txt");

            Blob blob = blobStore.blobBuilder(blobNames.get(i))//
                    .payload(contents.get(i)) //
                    .build();

            etags.add(blobStore.putBlob(container, blob));
        }
        Thread.sleep(TIME_TO_WAIT);

        /*
         * Should list all files
         */
        {
            PageSet<? extends StorageMetadata> pageSet = blobStore.list(container, ListContainerOptions.NONE);
            assertEquals(TEST_FILES, pageSet.size());
            List<String> names = pageSet.stream().map(sm -> sm.getName()).collect(Collectors.toList());
            assertTrue(names.containsAll(blobNames));
        }

        /*
         * Should list all files with prefix pre-
         */
        {
            PageSet<? extends StorageMetadata> pageSet = blobStore.list(container,
                    ListContainerOptions.Builder.prefix("pre-"));
            assertEquals(TEST_FILES / 2, pageSet.size());
            List<String> names = pageSet.stream().map(sm -> sm.getName()).collect(Collectors.toList());
            List<String> preFixBlobNames = blobNames.stream().filter(f -> f.startsWith("pre-"))
                    .collect(Collectors.toList());
            assertTrue(names.containsAll(preFixBlobNames));
        }

        /*
         * Should support pagination
         */
        {
            PageSet<? extends StorageMetadata> firstPageSet = blobStore.list(container,
                    ListContainerOptions.Builder.maxResults(TEST_FILES / 2));
            assertNotNull(firstPageSet.getNextMarker());

            PageSet<? extends StorageMetadata> secondPageSet = blobStore.list(container,
                    ListContainerOptions.Builder.maxResults(TEST_FILES / 2).afterMarker(firstPageSet.getNextMarker()));
            assertEquals(TEST_FILES / 2, secondPageSet.size());

            Set<String> names = new HashSet<>();
            names.addAll(firstPageSet.stream().map(sm -> sm.getName()).collect(Collectors.toList()));
            names.addAll(secondPageSet.stream().map(sm -> sm.getName()).collect(Collectors.toList()));

            assertTrue(names.containsAll(blobNames));
        }

    }
}
