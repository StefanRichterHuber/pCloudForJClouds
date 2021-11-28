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
	public void shouldUploadAndDownloadContent() throws IOException {
		BlobStore blobStore = getBlobStore();
		String container = UUID.randomUUID().toString();
		String blobName = UUID.randomUUID().toString() + ".txt";
		String blobContent = UUID.randomUUID().toString();

		// Create random container
		blobStore.createContainerInLocation(null, container);
		assertTrue(blobStore.containerExists(container));

		// Upload content
		Blob blob = blobStore.blobBuilder(blobName)//
				.payload(blobContent) //
				.build();

		String etag = blobStore.putBlob(container, blob);
		assertNotNull("Should have an etag", etag);

		// Download content
		Blob result = blobStore.getBlob(container, blobName, null);
		assertNotNull(result);
		String resultContent = IOUtils.toString(result.getPayload().openStream(), StandardCharsets.UTF_8.name());
		assertEquals("Content should be equal", blobContent, resultContent);

		// Delete blob
		blobStore.removeBlob(container, blobName);

	}

}
