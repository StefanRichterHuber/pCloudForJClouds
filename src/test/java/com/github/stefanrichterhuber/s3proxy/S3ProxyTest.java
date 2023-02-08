package com.github.stefanrichterhuber.s3proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.base.Charsets;

public class S3ProxyTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(S3ProxyTest.class);

    private AmazonS3 s3Client;

    private S3Proxy s3Proxy;

    private static final List<String> CONTENT_LINES = Arrays.asList("O rose, thou art sick!\r\n",
            "The invisible worm,\r\n", "That flies in the night,\r\n", "In the howling storm.\r\n",
            "Has found out thy bed\r\n", "Of crimson joy,\r\n", "And his dark secret love\r\n",
            "Does thy life destroy.");
    private static final String CONTENT = CONTENT_LINES.stream().collect(Collectors.joining());
    private static final byte[] CONTENT_BYTES = CONTENT.getBytes(Charsets.UTF_8);

    @SuppressWarnings("rawtypes")
    @Rule
    public GenericContainer<?> redis = new GenericContainer(DockerImageName.parse("redis:5.0.3-alpine"))
            .withExposedPorts(6379);

    @Before
    public void setup() throws Exception {
        final String token = System.getenv("PCLOUD_TOKEN");

        s3Proxy = S3Proxy.builder() //
                .endpoint(URI.create("http://127.0.0.1:8080")) //
                .awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "access", "secret") //
                .build();

        Properties properties = new Properties();
        properties.setProperty(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING,
                String.format("redis://%s:%d", redis.getHost(), redis.getFirstMappedPort()));
        properties.setProperty(PCloudConstants.PROPERTY_BASEDIR, "/S3");

        s3Proxy.setBlobStoreLocator(new DynamicPCloudBlobStoreLocator(properties));
        s3Proxy.start();
        while (!s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
            Thread.sleep(1);
        }

        s3Client = AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
                .withCredentials(new AWSStaticCredentialsProvider(
                        // User ID and secretkey must be the same: The pCloud secret key
                        new BasicAWSCredentials(token,
                                token)))
                // Region does not matter here
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:8080",
                        Regions.US_EAST_1.getName()))
                .build();

    }

    @After
    public void close() throws Exception {
        this.s3Client.shutdown();
        this.s3Proxy.stop();
    }

    @Test
    public void shouldList() {
        String bucket = UUID.randomUUID().toString();

        s3Client.createBucket(bucket);

        for (char i = 'a'; i <= 'j'; i++) {
            ObjectMetadata om = new ObjectMetadata();
            om.setContentLength(CONTENT_BYTES.length);
            String key = i + "blob";
            s3Client.putObject(new PutObjectRequest(bucket, key, new ByteArrayInputStream(CONTENT_BYTES), om));
        }

        ObjectListing objectListing1 = s3Client
                .listObjects(new ListObjectsRequest().withBucketName(bucket).withMaxKeys(5));
        List<S3ObjectSummary> objectSummaries1 = objectListing1.getObjectSummaries();
        assertFalse(objectSummaries1.isEmpty());
        assertNotNull(objectListing1.getNextMarker());

        ObjectListing objectListing2 = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucket)
                .withMaxKeys(5).withMarker(objectListing1.getNextMarker()));
        List<S3ObjectSummary> objectSummaries2 = objectListing2.getObjectSummaries();
        assertFalse(objectSummaries2.isEmpty());

        // Check if there are no overlaps
        List<String> r1 = objectSummaries1.stream().map(os -> os.getKey()).collect(Collectors.toList());
        List<String> r2 = objectSummaries2.stream().map(os -> os.getKey()).collect(Collectors.toList());

        for (String r : r1) {
            assertFalse(r2.contains(r));
        }

        // Remove all content of the bucket
        for (char i = 'a'; i <= 'j'; i++) {
            ObjectMetadata om = new ObjectMetadata();
            om.setContentLength(CONTENT_BYTES.length);
            String key = i + "blob";
            s3Client.deleteObject(new DeleteObjectRequest(bucket, key));
        }
        s3Client.deleteBucket(bucket);
    }

    @Test
    public void shouldPutAndGetContent() throws IOException, InterruptedException {
        Map<String, String> md = new HashMap<>();
        md.put("Usermetadata1", "user meta data value1");
        String bucket = UUID.randomUUID().toString();
        String key = UUID.randomUUID().toString();

        s3Client.createBucket(bucket);

        // Upload
        ObjectMetadata om = new ObjectMetadata();
        om.setUserMetadata(md);
        om.setContentLength(CONTENT_BYTES.length);
        s3Client.putObject(new PutObjectRequest(bucket, key, new ByteArrayInputStream(CONTENT_BYTES), om));

        // Download and check content
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
        try (InputStream inputStream = s3Object.getObjectContent()) {
            byte[] resultArray = IOUtils.toByteArray(inputStream);
            assertEquals(CONTENT, new String(resultArray, Charsets.UTF_8));
        }
        assertEquals(md, s3Object.getObjectMetadata().getUserMetadata());

        // Remove all content of the bucket
        s3Client.deleteObject(new DeleteObjectRequest(bucket, key));
        s3Client.deleteBucket(bucket);
    }

    @Test
    public void shouldDoS3Multipart() throws Exception {
        // Create 8 parts with 6M (must be at least 5M) of random content each - yeah
        // its
        // stupid, but it works... :/
        List<byte[]> contents = new ArrayList<>();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(8 * 6_000_000);
        for (int i = 0; i < 8; i++) {
            byte[] content = new byte[6_000_000];
            for (int j = 0; j < 6_000_000; j++) {
                content[j] = (byte) (Math.random() * 255.0);
            }
            baos.write(content);
            contents.add(content);
        }
        byte[] content = baos.toByteArray();

        String bucket = UUID.randomUUID().toString();
        String key = UUID.randomUUID().toString();
        Map<String, String> md = new HashMap<>();
        md.put("Usermetadata1", "user meta data value1");

        s3Client.createBucket(bucket);

        LOGGER.info("Uploading to key {} to bucket", key, bucket);

        ObjectMetadata omd = new ObjectMetadata();
        omd.setUserMetadata(md);
        InitiateMultipartUploadResult initiateMultipartUpload = s3Client
                .initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key, omd) //
                        .withObjectMetadata(omd));

        List<PartETag> partEtags = new ArrayList<>();
        for (int i = 0; i < contents.size(); i++) {

            UploadPartResult part = s3Client.uploadPart(new UploadPartRequest() //
                    .withBucketName(bucket) //
                    .withKey("key" + i).withUploadId(initiateMultipartUpload.getUploadId()) //
                    .withInputStream(new ByteArrayInputStream(contents.get(i))) //
                    .withPartSize(contents.get(i).length).withPartNumber(i + 1));

            partEtags.add(part.getPartETag());
        }

        s3Client.completeMultipartUpload(
                new CompleteMultipartUploadRequest().withUploadId(initiateMultipartUpload.getUploadId())
                        .withBucketName(bucket).withKey("final").withPartETags(partEtags));

        Thread.sleep(1000);

        // Download and check
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
        try (InputStream inputStream = s3Object.getObjectContent()) {
            byte[] resultArray = IOUtils.toByteArray(inputStream);
            assertEquals(content.length, resultArray.length);
            Assert.assertArrayEquals(content, resultArray);
        }
        assertEquals(md, s3Object.getObjectMetadata().getUserMetadata());

        s3Client.deleteObject(new DeleteObjectRequest(bucket, key));
        s3Client.deleteBucket(bucket);
    }
}
