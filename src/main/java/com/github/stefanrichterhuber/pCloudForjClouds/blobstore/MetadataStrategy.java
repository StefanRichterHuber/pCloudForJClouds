package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Strategy to handle user defined metadata
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
	CompletableFuture<Map<String, String>> get(String container, String key);

	/**
	 * Persist some metadat for the given blob
	 * 
	 * @param container Container containing the blob
	 * @param key       Key of the blob
	 * @param metadata  Metadata to persist for the given blob
	 * @return {@link CompletableFuture} of the operation.
	 */
	CompletableFuture<Void> put(String container, String key, Map<String, String> metadata);

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
	 * @return {@link CompletableFuture} of the operation.
	 */
	default CompletableFuture<Void> copy(String fromContainer, String fromKey, String toContainer, String toKey) {
		return this.get(fromContainer, fromKey).thenAcceptAsync(metadata -> this.put(toContainer, toKey, metadata));
	}
}
