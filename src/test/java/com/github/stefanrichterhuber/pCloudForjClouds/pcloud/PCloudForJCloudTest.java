package com.github.stefanrichterhuber.pCloudForjClouds.pcloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.base.Charsets;

public class PCloudForJCloudTest {
	private final static Logger LOGGER = LoggerFactory.getLogger(PCloudForJCloudTest.class);

	private static final int TIME_TO_WAIT = 750;

	private static BlobStore getBlobStore() {
		Properties properties = new Properties();
		properties.setProperty(PCloudConstants.PROPERTY_BASEDIR, "/S3");
		properties.setProperty(PCloudConstants.PROPERTY_CLIENT_SECRET, System.getenv("PCLOUD_TOKEN"));

		BlobStoreContext context = ContextBuilder.newBuilder("pcloud").overrides(properties)
				.build(BlobStoreContext.class);

		BlobStore blobStore = context.getBlobStore();
		return blobStore;
	}

	@Test
	public void shouldCreateAndDestroyContainer() throws InterruptedException {
		BlobStore blobStore = getBlobStore();
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
	public void shouldCreateFoldersForBlob() throws InterruptedException {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		String blobName = "f1/f2/f3/f4/" + UUID.randomUUID().toString() + ".txt";
		String blobContent = UUID.randomUUID().toString();

		blobStore.createContainerInLocation(null, container);
		Thread.sleep(TIME_TO_WAIT);
		assertTrue(blobStore.containerExists(container));

		Blob contentBlob = blobStore.blobBuilder(blobName) //
				.payload(blobContent) //
				.build();
		blobStore.putBlob(container, contentBlob);
		Thread.sleep(TIME_TO_WAIT);
		blobStore.blobExists(container, blobName);

		blobStore.deleteContainer(container);
	}

	@Test
	public void shouldDeleteAFullContainer() throws InterruptedException {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		String b1Name = UUID.randomUUID().toString();
		String b2Name = UUID.randomUUID().toString();
		String b3Name = UUID.randomUUID().toString();

		blobStore.createContainerInLocation(null, container);

		assertTrue(blobStore.containerExists(container));

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

		blobStore.deleteContainer(container);
		Thread.sleep(TIME_TO_WAIT);
		assertFalse(blobStore.containerExists(container));

	}

	@Test
	public void shouldCreateDirectoryInContainer() throws InterruptedException {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		String folder = UUID.randomUUID().toString() + "/";
		String blobName = UUID.randomUUID().toString() + ".txt";
		String blobContent = UUID.randomUUID().toString();

		// Create test container
		blobStore.createContainerInLocation(null, container);
		assertTrue(blobStore.containerExists(container));

		// Create test folder
		Blob folderBlob = blobStore //
				.blobBuilder(folder) //
				.build();
		blobStore.putBlob(container, folderBlob);
		Thread.sleep(TIME_TO_WAIT);
		// Check if test folder is present
		assertTrue(blobStore.blobExists(container, folder));

		// Insert file into test folder
		Blob contentBlob = blobStore.blobBuilder(folder + blobName) //
				.payload(blobContent) //
				.build();

		blobStore.putBlob(container, contentBlob);
		Thread.sleep(TIME_TO_WAIT);
		// Check if test content is present
		assertTrue(blobStore.blobExists(container, folder + blobName));

		// Cleanup
		blobStore.deleteContainer(container);

	}

	@Test
	public void shouldClearAContainer() throws InterruptedException {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		String b1Name = UUID.randomUUID().toString();
		String b2Name = UUID.randomUUID().toString();
		String b3Name = UUID.randomUUID().toString();

		blobStore.createContainerInLocation(null, container);

		assertTrue(blobStore.containerExists(container));

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

		// Clean up and destroy the test container
		blobStore.deleteContainer(container);
		Thread.sleep(TIME_TO_WAIT);
		assertFalse(blobStore.containerExists(container));
	}

	@Test
	public void shouldListContainerNames() throws InterruptedException {
		BlobStore blobStore = getBlobStore();
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
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		String blobName = UUID.randomUUID().toString() + ".txt";
		String blobContent = UUID.randomUUID().toString();

		// Create random container
		blobStore.createContainerInLocation(null, container);
		assertTrue(blobStore.containerExists(container));

		// Blob should not exist
		assertFalse(blobStore.blobExists(container, blobName));

		// Upload content
		Blob blob = blobStore.blobBuilder(blobName)//
				.payload(blobContent) //
				.build();

		String etag = blobStore.putBlob(container, blob);
		assertNotNull("Should have an etag", etag);
		Thread.sleep(TIME_TO_WAIT);

		// Blob should exist
		assertTrue(blobStore.blobExists(container, blobName));

		// Download content
		Blob result = blobStore.getBlob(container, blobName, GetOptions.NONE);
		assertNotNull(result);
		String resultContent = IOUtils.toString(result.getPayload().openStream(), StandardCharsets.UTF_8.name());
		assertEquals("Content should be equal", blobContent, resultContent);

		// Delete blob
		blobStore.removeBlob(container, blobName);
		Thread.sleep(TIME_TO_WAIT);

		// Blob should be deleted
		assertFalse(blobStore.blobExists(container, blobName));
		assertNull(blobStore.getBlob(container, blobName));
		blobStore.deleteContainer(container);

	}

	@Test
	public void shouldCopyBlob() throws InterruptedException, IOException {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();

		String sourceBlobName = UUID.randomUUID().toString() + ".txt";
		String blobContent = UUID.randomUUID().toString();

		String targetBlobName = UUID.randomUUID().toString() + ".txt";

		// Create random container
		blobStore.createContainerInLocation(null, container);
		assertTrue(blobStore.containerExists(container));

		// Upload content
		Blob blob = blobStore.blobBuilder(sourceBlobName)//
				.payload(blobContent) //
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

		// Delete blobs
		blobStore.removeBlob(container, sourceBlobName);
		blobStore.removeBlob(container, targetBlobName);
		blobStore.deleteContainer(container);
	}

	@Test
	public void shouldSupportMultiPartUpload() throws InterruptedException, IOException {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		List<String> content = Arrays.asList("O rose, thou art sick!\r\n", "The invisible worm,\r\n",
				"That flies in the night,\r\n", "In the howling storm.\r\n", "Has found out thy bed\r\n",
				"Of crimson joy,\r\n", "And his dark secret love\r\n", "Does thy life destroy.");
		String mergedContent = content.stream().collect(Collectors.joining());
		String blobName = UUID.randomUUID().toString() + ".txt";

		LOGGER.info("Uploading to blob {} in container {}", blobName, container);

		blobStore.createContainerInLocation(null, container);
		assertTrue(blobStore.containerExists(container));
		Thread.sleep(TIME_TO_WAIT);

		MutableBlobMetadata metaData = blobStore.blobBuilder(blobName).payload(mergedContent)
				.contentLength(mergedContent.getBytes(Charsets.UTF_8).length).build().getMetadata();
		MultipartUpload multipartUpload = blobStore.initiateMultipartUpload(container, metaData, null);
		assertNotNull(multipartUpload);

		for (int i = 0; i < content.size(); i++) {
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

		blobStore.deleteContainer(container);
	}

	/**
	 * Test multipart upload, but not all parts appear in order
	 * 
	 * @throws InterruptedException
	 * @throws IOException
	 */
	@Test
	public void shouldSupportMultiPartUploadWithRandomOrder() throws InterruptedException, IOException {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		List<String> content = Arrays.asList("O rose, thou art sick!\r\n", "The invisible worm,\r\n",
				"That flies in the night,\r\n", "In the howling storm.\r\n", "Has found out thy bed\r\n",
				"Of crimson joy,\r\n", "And his dark secret love\r\n", "Does thy life destroy.");
		String mergedContent = content.stream().collect(Collectors.joining());
		long contentLength = mergedContent.getBytes(Charsets.UTF_8).length;
		String blobName = UUID.randomUUID().toString() + ".txt";

		LOGGER.info("Uploading to blob {} in container {}", blobName, container);

		blobStore.createContainerInLocation(null, container);
		assertTrue(blobStore.containerExists(container));
		Thread.sleep(TIME_TO_WAIT);

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

		blobStore.deleteContainer(container);
	}

	@Test
	public void shouldListMetadataRecursivly() throws InterruptedException {

		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();

		LOGGER.info("Uploading to container {}", container);

		blobStore.createContainerInLocation(null, container);
		Thread.sleep(TIME_TO_WAIT);
		assertTrue(blobStore.containerExists(container));

		String blobName1 = UUID.randomUUID().toString() + ".txt";
		String blobName2 = "pre-" + UUID.randomUUID().toString() + ".txt";

		String blobName3 = "rnd/" + UUID.randomUUID().toString() + ".txt";
		String blobName4 = "rnd/" + "pre-" + UUID.randomUUID().toString() + ".txt";

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
			assertTrue(names.containsAll(Arrays.asList(blobName1, blobName2, blobName3.substring("rnd/".length()), blobName4.substring("rnd/".length()))));
		}

		/*
		 * Should list all files with prefix since recursive
		 */
		{
			PageSet<? extends StorageMetadata> pageSet = blobStore.list(container,
					ListContainerOptions.Builder.recursive().prefix("pre-"));
			assertEquals(2, pageSet.size());
			List<String> names = pageSet.stream().map(sm -> sm.getName()).collect(Collectors.toList());
			assertTrue(names.containsAll(Arrays.asList(blobName2, blobName4.substring("rnd/".length()))));
		}

		blobStore.deleteContainer(container);

	}

	@Test
	public void shouldListMetadata() throws InterruptedException {
		final int TEST_FILES = 10;

		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();

		LOGGER.info("Uploading to container {}", container);

		blobStore.createContainerInLocation(null, container);
		Thread.sleep(TIME_TO_WAIT);
		assertTrue(blobStore.containerExists(container));

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

		blobStore.deleteContainer(container);
	}

}
