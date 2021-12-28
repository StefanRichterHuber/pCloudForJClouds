package com.github.stefanrichterhuber.s3proxy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
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

	@Before
	public void setup() throws Exception {

		/**
		 * AWS client expects MD5 hash while pcloud delivers sha hashes, so disable MD5
		 * validation
		 */
		System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");
		System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");

		s3Proxy = S3Proxy.builder() //
				.endpoint(URI.create("http://127.0.0.1:8080")) //
				.awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "access", "secret") //
				.build();

		Properties properties = new Properties();
		properties.setProperty(PCloudConstants.PROPERTY_BASEDIR, "/S3");

		s3Proxy.setBlobStoreLocator(new DynamicPCloudBlobStoreLocator(properties));
		s3Proxy.start();
		while (!s3Proxy.getState().equals(AbstractLifeCycle.STARTED)) {
			Thread.sleep(1);
		}

		s3Client = AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
				.withCredentials(new AWSStaticCredentialsProvider(
						new BasicAWSCredentials(System.getenv("PCLOUD_TOKEN"), System.getenv("PCLOUD_TOKEN"))))
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
		
		for(char i = 'a'; i<='j'; i++) {
			ObjectMetadata om = new ObjectMetadata();
			om.setContentLength(CONTENT_BYTES.length);
			String key = i + "blob";
			s3Client.putObject(new PutObjectRequest(bucket, key, new ByteArrayInputStream(CONTENT_BYTES), om));
		}
		
		
		ObjectListing objectListing1 = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucket).withMaxKeys(5));
		List<S3ObjectSummary> objectSummaries1 = objectListing1.getObjectSummaries();
		assertFalse(objectSummaries1.isEmpty());
		assertEquals(5, objectSummaries1.size());
		assertNotNull(objectListing1.getNextMarker());
		
		
		ObjectListing objectListing2 = s3Client.listObjects(new ListObjectsRequest().withBucketName(bucket).withMaxKeys(5).withMarker(objectListing1.getNextMarker()));
		List<S3ObjectSummary> objectSummaries2 = objectListing2.getObjectSummaries();
		assertFalse(objectSummaries2.isEmpty());
		assertEquals(5, objectSummaries2.size());
		
		// Check if there are no overlaps
		List<String> r1 = objectSummaries1.stream().map(os -> os.getKey()).collect(Collectors.toList());
		List<String> r2 = objectSummaries2.stream().map(os -> os.getKey()).collect(Collectors.toList());
		
		for(String r: r1) {
			assertFalse(r2.contains(r));
		}
		
		s3Client.deleteBucket(bucket);
	}

	@Test
	public void shouldPutAndGetContent() throws IOException {

		String bucket = UUID.randomUUID().toString();
		String key = UUID.randomUUID().toString();

		s3Client.createBucket(bucket);

		// Upload
		ObjectMetadata om = new ObjectMetadata();
		om.setContentLength(CONTENT_BYTES.length);
		s3Client.putObject(new PutObjectRequest(bucket, key, new ByteArrayInputStream(CONTENT_BYTES), om));

		// Download and check content
		S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
		try (InputStream inputStream = s3Object.getObjectContent()) {
			byte[] resultArray = IOUtils.toByteArray(inputStream);
			assertEquals(CONTENT, new String(resultArray, Charsets.UTF_8));
		}

		s3Client.deleteBucket(bucket);
	}

	@Test
	public void shouldDoS3Multipart() throws Exception {
		String bucket = UUID.randomUUID().toString();
		String key = UUID.randomUUID().toString();

		s3Client.createBucket(bucket);

		LOGGER.info("Uploading to key {} to bucket", key, bucket);

		ObjectMetadata omd = new ObjectMetadata();
		InitiateMultipartUploadResult initiateMultipartUpload = s3Client
				.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key, omd) //
						.withObjectMetadata(omd));

		List<PartETag> partEtags = new ArrayList<>();
		for (int i = 0; i < CONTENT_LINES.size(); i++) {

			UploadPartResult part = s3Client.uploadPart(new UploadPartRequest() //
					.withBucketName(bucket) //
					.withKey("key" + i).withUploadId(initiateMultipartUpload.getUploadId()) //
					.withInputStream(new ByteArrayInputStream(CONTENT_LINES.get(i).getBytes(Charsets.UTF_8))) //
					.withPartSize(CONTENT_LINES.get(i).getBytes(Charsets.UTF_8).length).withPartNumber(i + 1));
			partEtags.add(part.getPartETag());
		}

		s3Client.completeMultipartUpload(
				new CompleteMultipartUploadRequest().withUploadId(initiateMultipartUpload.getUploadId())
						.withBucketName(bucket).withKey("final").withPartETags(partEtags));

		// Download and check
		S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucket, key));
		try (InputStream inputStream = s3Object.getObjectContent()) {
			byte[] resultArray = IOUtils.toByteArray(inputStream);
			assertEquals(CONTENT, new String(resultArray, Charsets.UTF_8));
		}

		s3Client.deleteBucket(bucket);
	}
}
