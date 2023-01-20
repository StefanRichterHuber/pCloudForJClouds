package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

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
    public static final ExternalBlobMetadata EMPTY_METADATA = new ExternalBlobMetadata(null, Collections.emptyMap());

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
     * Copies the metadata for one blob to the metadata of another blob
     * 
     * @param fromContainer Source container
     * @param fromKey       Source blob name
     * @param toContainer   Target container
     * @param toKey         Target container name
     * @return {@link ExternalBlobMetadata} of the target file
     */
    default CompletableFuture<ExternalBlobMetadata> copy(String fromContainer, String fromKey, String toContainer,
            String toKey) {
        return this.get(fromContainer, fromKey)
                .thenCompose(metadata -> this.put(toContainer, toKey, metadata).thenApply(v -> metadata));
    }

    /**
     * Moves the metadata for one blob to the metadata of another blob
     * 
     * @param fromContainer Source container
     * @param fromKey       Source blob name
     * @param toContainer   Target container
     * @param toKey         Target container name
     * @return {@link ExternalBlobMetadata} of the target file
     */
    default CompletableFuture<ExternalBlobMetadata> move(String fromContainer, String fromKey, String toContainer,
            String toKey) {
        return this.get(fromContainer, fromKey)
                .thenCompose(metadata -> put(toContainer, toKey, metadata)
                        .thenCompose(v -> this.delete(fromContainer, fromKey))
                        .thenApply(v -> metadata));
    }
}
