package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.options.ListContainerOptions;

/**
 * Strategy to handle user defined metadata
 * 
 * @author Stefan Richter-Huber
 *
 */
public interface MetadataStrategy {
    /**
     * Empty metadata
     */
    public static final ExternalBlobMetadata EMPTY_METADATA = new ExternalBlobMetadata(null, null, 0,
            BlobAccess.PRIVATE,
            BlobHashes.empty(),
            Collections.emptyMap());

    /**
     * Retrieves the metadata for the given blob
     * 
     * @param container Container containing the blob
     * @param key       Key of the blob
     * @return {@link CompletableFuture} containing the result of the operation.
     */
    CompletableFuture<ExternalBlobMetadata> get(String container, String key);

    /**
     * Persist some metadata for the given blob
     * 
     * @param container Container containing the blob
     * @param key       Key of the blob
     * @param metadata  {@link ExternalBlobMetadata} to persist for the given blob
     * @return {@link CompletableFuture} of the operation.
     */
    CompletableFuture<Void> put(String container, String key, ExternalBlobMetadata metadata);

    /**
     * Deletes the metadata for a blob
     * 
     * @param container Container containing the blob
     * @param key       Key of the blob
     * @return {@link CompletableFuture} of the operation.
     */
    CompletableFuture<Void> delete(String container, String key);

    /**
     * Lists all metadata keys for the given container mathing the given
     * {@link ListContainerOptions}.
     * 
     * @param containerName Container to scan
     * @param options       {@link ListContainerOptions} to apply
     * @return
     */
    CompletableFuture<PageSet<ExternalBlobMetadata>> list(String containerName,
            ListContainerOptions options);
}
