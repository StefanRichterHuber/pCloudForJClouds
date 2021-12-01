package eu.stefanhuber.jclouds;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.gaul.s3proxy.AuthenticationType;
import org.gaul.s3proxy.S3Proxy;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.junit.Test;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.Authenticators;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.PCloudSdk;
import com.pcloud.sdk.RemoteFile;

import eu.stefanhuber.jclouds.pcloud.reference.PCloudConstants;
import eu.stefanhuber.s3proxy.DynamicPCloudBlobStoreLocator;

public class Benchmark {
	private static final int contentSize = 100;
	private static final int runs = 20;

	final byte[] blobContent = createContent();

	final String baseUrl = "https://webdav.pcloud.com/S3/";

	private static byte[] createContent() {
		byte[] b = new byte[contentSize];
		new Random().nextBytes(b);
		return b;
	}

	public void webdavRun(Sardine sardine) throws IOException {
		final String container = "sardineBenchmark";
		final String containerUrl = baseUrl + container + "/";
		final String blobName = UUID.randomUUID().toString() + ".txt";
		final String blobUrl = containerUrl + blobName;

		sardine.put(blobUrl, blobContent);

		try (InputStream inputStream = sardine.get(blobUrl)) {
			byte[] resultArray = IOUtils.toByteArray(inputStream);
			if (!Arrays.equals(blobContent, resultArray)) {
				throw new RuntimeException("Content not equal");
			}
		}

	}

	public void pcloudApiClientRun(ApiClient apiClient) throws IOException, ApiError {
		final String container = "pcloudBenchmark";
		final String containerUrl = "/S3/" + container;
		final String blobName = UUID.randomUUID().toString() + ".txt";
		final String blobUrl = containerUrl + "/" + blobName;
		RemoteFile remoteFile = apiClient.createFile(containerUrl, blobName, DataSource.create(blobContent)).execute();

		RemoteFile result = apiClient.loadFile(blobUrl).execute();
		try (InputStream inputStream = result.byteStream()) {
			byte[] resultArray = IOUtils.toByteArray(inputStream);
			if (!Arrays.equals(blobContent, resultArray)) {
				throw new RuntimeException("Content not equal");
			}
		}
	}

	public void blobStoreRun(BlobStore blobStore) throws IOException {
		final String container = "pcloudBlobstoreBenchmark";
		final String blobName = UUID.randomUUID().toString() + ".txt";

		String etag = blobStore.putBlob(container, blobStore.blobBuilder(blobName).payload(blobContent).build());

		Blob blob = blobStore.getBlob(container, blobName, null);
		try (InputStream inputStream = blob.getPayload().openStream()) {
			byte[] resultArray = IOUtils.toByteArray(inputStream);
			if (!Arrays.equals(blobContent, resultArray)) {
				throw new RuntimeException("Content not equal");
			}
		}
	}

	public void s3ClientRun(AmazonS3 s3Client) throws IOException {
		final String container = "s3benchmark";
		final String blobName = UUID.randomUUID().toString() + ".txt";

		ObjectMetadata objectMetadata = new ObjectMetadata();
		objectMetadata.setContentLength(contentSize);
		s3Client.putObject(
				new PutObjectRequest(container, blobName, new ByteArrayInputStream(blobContent), objectMetadata));

		S3Object s3Object = s3Client.getObject(new GetObjectRequest(container, blobName));
		try (InputStream inputStream = s3Object.getObjectContent()) {
			byte[] resultArray = IOUtils.toByteArray(inputStream);
			if (!Arrays.equals(blobContent, resultArray)) {
				throw new RuntimeException("Content not equal");
			}
		}
	}

	@Test
	public void webDav() throws IOException {
		final Sardine sardine = SardineFactory.begin(System.getenv("PCLOUD_USER"), System.getenv("PCLOUD_PASSWORD"));

		System.out.println("Start webdav run");
		System.out.println("   Warmup");
		// Warmup
		for (int i = 0; i < 10; i++) {
			webdavRun(sardine);
		}
		System.out.println("   Warmup done");

		// Benchmark
		long start = System.currentTimeMillis();
		for (int i = 0; i < runs; i++) {
			webdavRun(sardine);
		}
		long end = System.currentTimeMillis();
		long diff = end - start;
		long perRun = diff / runs;
		System.out.println("webdav test with " + runs + " runs and " + contentSize + " bytes took " + diff + " ms. "
				+ perRun + " ms/run");
	}

	@Test
	public void pcloud() throws IOException, ApiError {
		final ApiClient apiClient = PCloudSdk.newClientBuilder()
				.authenticator(Authenticators.newOAuthAuthenticator(System.getenv("PCLOUD_TOKEN"))).create();
		System.out.println("Start pcloud run");
		System.out.println("   Warmup");

		// Warmup
		for (int i = 0; i < 10; i++) {
			pcloudApiClientRun(apiClient);
		}
		System.out.println("   Warmup done");

		// Benchmark
		long start = System.currentTimeMillis();
		for (int i = 0; i < runs; i++) {
			pcloudApiClientRun(apiClient);
		}
		long end = System.currentTimeMillis();
		long diff = end - start;
		long perRun = diff / runs;
		System.out.println("pcloud test with " + runs + " runs and " + contentSize + " bytes took " + diff + " ms. "
				+ perRun + " ms/run");
	}

	@Test
	public void pcloudBlobStore() throws IOException {
		Properties properties = new Properties();
		properties.setProperty(PCloudConstants.PROPERTY_BASEDIR, "/S3");
		properties.setProperty(PCloudConstants.PROPERTY_CLIENT_SECRET, System.getenv("PCLOUD_TOKEN"));

		BlobStoreContext context = ContextBuilder.newBuilder("pcloud").overrides(properties)
				.build(BlobStoreContext.class);

		BlobStore blobStore = context.getBlobStore();

		System.out.println("Start pcloud blobstore run");
		System.out.println("   Warmup");

		// Warmup
		for (int i = 0; i < 10; i++) {
			blobStoreRun(blobStore);
		}
		System.out.println("   Warmup done");

		// Benchmark
		long start = System.currentTimeMillis();
		for (int i = 0; i < runs; i++) {
			blobStoreRun(blobStore);
		}
		long end = System.currentTimeMillis();
		long diff = end - start;
		long perRun = diff / runs;
		System.out.println("pcloud blobstore test with " + runs + " runs and " + contentSize + " bytes took " + diff
				+ " ms. " + perRun + " ms/run");

	}

	@Test
	public void s3ViaPCloud() throws Exception {

		/**
		 * AWS client expects MD5 hash while pcloud delivers sha hashes, so disable MD5
		 * validation
		 */
		System.setProperty("com.amazonaws.services.s3.disablePutObjectMD5Validation", "true");
		System.setProperty("com.amazonaws.services.s3.disableGetObjectMD5Validation", "true");

		S3Proxy s3Proxy = S3Proxy.builder() //
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

		AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(System.getenv("PCLOUD_TOKEN"), System.getenv("PCLOUD_TOKEN"))))
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://127.0.0.1:8080",
						Regions.US_EAST_1.getName()))
				.build();

		// Warmup
		System.out.println("Start s3 blobstore run");
		for (int i = 0; i < 10; i++) {
			s3ClientRun(s3Client);
		}
		System.out.println("   Warmup done");

		// Benchmark
		long start = System.currentTimeMillis();
		for (int i = 0; i < runs; i++) {
			s3ClientRun(s3Client);
		}
		long end = System.currentTimeMillis();
		long diff = end - start;
		long perRun = diff / runs;
		System.out.println("s3 test with " + runs + " runs and " + contentSize + " bytes took " + diff
				+ " ms. " + perRun + " ms/run");

		s3Client.shutdown();
		s3Proxy.stop();
	}
}
