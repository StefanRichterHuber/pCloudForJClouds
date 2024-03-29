package com.github.stefanrichterhuber.s3proxy;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.gaul.s3proxy.BlobStoreLocator;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudUtils;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.collect.Maps;

/**
 * This S3Proxy {@link BlobStoreLocator} dynamically creates pCloud
 * {@link BlobStore} authenticated with the given identity. This way the s3proxy
 * does not have to known the pCloud secrets beforehand but retrieves it from
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

            // Determine which api instance to use
            if (!properties.containsKey(Constants.PROPERTY_ENDPOINT)) {
                final String apiEndpoint = PCloudUtils.testForAPIEndpoint(id).orNull();
                properties.setProperty(Constants.PROPERTY_ENDPOINT, apiEndpoint);
            }

            final BlobStoreContext context = ContextBuilder.newBuilder("pcloud").overrides(properties)
                    .build(BlobStoreContext.class);
            final BlobStore blobStore = context.getBlobStore();
            return Maps.immutableEntry(id, blobStore);
        });
        return result;
    }

}
