package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.options.ListContainerOptions;

import com.pcloud.sdk.RemoteEntry;
import com.pcloud.sdk.RemoteFile;

/**
 * Strategy to handle user defined metadata
 * 
 * @author Stefan Richter-Huber
 *
 */
public interface MetadataStrategy {
    /**
     * Retrieves the metadata for the given blob
     * 
     * @param container Container containing the blob
     * @param key       Key of the blob
     * @return {@link CompletableFuture} containing the result of the operation.
     */
    CompletableFuture<ExternalBlobMetadata> get(String container, String key);

    /**
     * Retrieves the metadata for the given blob. If not found, create a new one
     * using the given factory and insert it.
     * 
     * @param container Container containing the blob
     * @param key       Key of the blob
     * @param factory   If no value found, use this {@link Supplier} to create a new
     *                  {@link ExternalBlobMetadata}.
     * @return {@link CompletableFuture} containing the result of the operation.
     */
    default CompletableFuture<ExternalBlobMetadata> getOrCreate(String container, String key,
            BiFunction<String, String, CompletableFuture<ExternalBlobMetadata>> factory) {
        return this.get(container, key).thenCompose(em -> {
            if (em != null) {
                return CompletableFuture.completedFuture(em);
            } else {
                return factory.apply(container, key)
                        .thenCompose(emn -> this.put(container, key, emn).thenApply(v -> emn));
            }
        });
    }

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

    /**
     * Restores the metadata entries (without custom metadata!) for the given
     * {@link RemoteFile}. Data is stored in cache.
     * 
     * @param container    Container of the blob
     * @param key          Key of the blob
     * @param blobAccess   {@link BlobAccess} to set in metadata
     * @param usermetadata Additional user metadata
     * @param entry        {@link RemoteEntry} to generate the metadata for.
     * @return {@link ExternalBlobMetadata} generated and stored in cache.
     */
    CompletableFuture<ExternalBlobMetadata> restoreMetadata(String container, String key, BlobAccess blobAccess,
            Map<String, String> usermetadata, RemoteEntry entry);
}
