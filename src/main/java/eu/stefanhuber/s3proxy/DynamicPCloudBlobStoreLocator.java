package eu.stefanhuber.s3proxy;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.gaul.s3proxy.BlobStoreLocator;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;

import com.google.common.collect.Maps;

import eu.stefanhuber.jclouds.pcloud.reference.PCloudConstants;

/**
 * This S3Proxy {@link BlobStoreLocator} dynamically creates pcloud
 * {@link BlobStore} authenticated with the given identity. This way the s3proxy
 * does not have to known the pcloud secrets beforehand but retrieves it from
 * the s3 client. Access key and secret key from aws client must be both the
 * pcloud client secret!
 * 
 * @author Stefan Richter-Huber
 *
 */
public class DynamicPCloudBlobStoreLocator implements BlobStoreLocator {
	private final Map<String, Entry<String, BlobStore>> blobstores = new ConcurrentHashMap<>();
	private final Properties baseProperties;

	/**
	 * Creates a new {@link DynamicPCloudBlobStoreLocator} instance
	 * 
	 * @param properties Common configuration properties like
	 *                   {@link PCloudConstants#PROPERTY_BASEDIR} for all pcloud
	 *                   bases {@link BlobStore}s.
	 */
	public DynamicPCloudBlobStoreLocator(Properties properties) {
		this.baseProperties = properties;
	}

	@Override
	public Entry<String, BlobStore> locateBlobStore(String identity, String container, String blob) {
		final Entry<String, BlobStore> result = blobstores.computeIfAbsent(identity, id -> {
			final Properties properties = new Properties();
			/*
			 * Setting the baseProperties as defaults in the constructor does not work ->
			 * building the BlobStoreContext crashes. So simply copy them.
			 */
			for (Entry<Object, Object> entry : baseProperties.entrySet()) {
				properties.put(entry.getKey(), entry.getValue());
			}
			properties.setProperty(PCloudConstants.PROPERTY_CLIENT_SECRET, id);

			final BlobStoreContext context = ContextBuilder.newBuilder("pcloud").overrides(properties)
					.build(BlobStoreContext.class);
			final BlobStore blobStore = context.getBlobStore();
			return Maps.immutableEntry(identity, blobStore);
		});
		return result;
	}

}
