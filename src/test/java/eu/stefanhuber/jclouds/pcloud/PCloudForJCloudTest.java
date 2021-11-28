package eu.stefanhuber.jclouds.pcloud;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.junit.Test;

import eu.stefanhuber.jclouds.pcloud.reference.PCloudConstants;

public class PCloudForJCloudTest {

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
	public void shouldCreateAndDestroyContainer() {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();

		// Container should not exist at first
		assertFalse(blobStore.containerExists(container));

		// Container should be created
		blobStore.createContainerInLocation(null, container);

		// Container should be existing
		assertTrue(blobStore.containerExists(container));

		// Container should be deleted
		blobStore.deleteContainer(container);

		// Container should not exist in the end
		assertFalse(blobStore.containerExists(container));
	}
	
	@Test
	public void shouldDeleteAFullContainer() {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		String b1Name = UUID.randomUUID().toString();
		String b2Name = UUID.randomUUID().toString();
		String b3Name = UUID.randomUUID().toString();
		
		blobStore.createContainerInLocation(null, container);

		assertTrue(blobStore.containerExists(container));
		
		Blob b1 = blobStore.blobBuilder(b1Name)//
				.payload(b1Name)
				.build();
		Blob b2 = blobStore.blobBuilder(b2Name)//
				.payload(b2Name)
				.build();
		Blob b3 = blobStore.blobBuilder(b3Name)//
				.payload(b3Name)
				.build();
		
		blobStore.putBlob(container, b1);
		blobStore.putBlob(container, b2);
		blobStore.putBlob(container, b3);
		
		assertTrue(blobStore.blobExists(container, b1Name));
		assertTrue(blobStore.blobExists(container, b2Name));
		assertTrue(blobStore.blobExists(container, b3Name));
		
		blobStore.deleteContainer(container);
		assertFalse( blobStore.containerExists(container));
		
	}
	
	@Test
	public void shouldClearAContainer() {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		String b1Name = UUID.randomUUID().toString();
		String b2Name = UUID.randomUUID().toString();
		String b3Name = UUID.randomUUID().toString();
		
		blobStore.createContainerInLocation(null, container);

		assertTrue(blobStore.containerExists(container));
		
		Blob b1 = blobStore.blobBuilder(b1Name)//
				.payload(b1Name)
				.build();
		Blob b2 = blobStore.blobBuilder(b2Name)//
				.payload(b2Name)
				.build();
		Blob b3 = blobStore.blobBuilder(b3Name)//
				.payload(b3Name)
				.build();
		
		blobStore.putBlob(container, b1);
		blobStore.putBlob(container, b2);
		blobStore.putBlob(container, b3);
		
		assertTrue(blobStore.blobExists(container, b1Name));
		assertTrue(blobStore.blobExists(container, b2Name));
		assertTrue(blobStore.blobExists(container, b3Name));
		
		blobStore.clearContainer(container);
		assertTrue("Container should be still there", blobStore.containerExists(container));
		assertFalse(blobStore.blobExists(container, b1Name));
		assertFalse(blobStore.blobExists(container, b2Name));
		assertFalse(blobStore.blobExists(container, b3Name));
		
		// Clean up and destroy the test container
		blobStore.deleteContainerIfEmpty(container);

		assertFalse( blobStore.containerExists(container));
	}

	@Test
	public void shouldListContainerNames() {
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

		PageSet<? extends StorageMetadata> ps0 = blobStore.list();
		assertNotNull(ps0);
		assertTrue(ps0.size() == 3);

		for (StorageMetadata entry : ps0) {
			assertEquals(entry.getType(), StorageType.CONTAINER);
			assertTrue(entry.getName().equals(c1) || entry.getName().equals(c2) || entry.getName().equals(c3));
		}

		blobStore.deleteContainer(c1);
		blobStore.deleteContainer(c2);
		blobStore.deleteContainer(c3);

		PageSet<? extends StorageMetadata> ps1 = blobStore.list();
		assertNotNull(ps1);
		assertTrue(ps1.size() == 0);
	}

	@Test
	public void shouldUploadAndDownloadContent() throws IOException {
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

		// Blob should exist
		assertTrue(blobStore.blobExists(container, blobName));

		// Download content
		Blob result = blobStore.getBlob(container, blobName, null);
		assertNotNull(result);
		String resultContent = IOUtils.toString(result.getPayload().openStream(), StandardCharsets.UTF_8.name());
		assertEquals("Content should be equal", blobContent, resultContent);

		// Delete blob
		blobStore.removeBlob(container, blobName);

	}

}
