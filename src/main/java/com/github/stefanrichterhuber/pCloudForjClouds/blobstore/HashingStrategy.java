package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.jclouds.blobstore.domain.Blob;

import com.google.common.collect.ImmutableMap;
import com.pcloud.sdk.DataSource;

/**
 * Strategy to calculat and provided hashes
 */
public interface HashingStrategy {
    public static interface Hashes {
         static final String PROPERTY_MD5 = "md5";
         static final String PROPERTY_SHA1 = "sha-1";

        /*
         * MD5 hash of the target
         */
        String md5();
        /**
         * SHA-1 hash of the target
         * @return
         */
        String sha1();
        
        /**
         * Converts this object to a {@link Map}.
         * @return
         */
        default Map<String, String> toMap() {
            return ImmutableMap.of(PROPERTY_MD5, md5(), PROPERTY_SHA1, sha1());
        }
    }

    CompletableFuture<Hashes> get(Blob blob);

    /**
     * Deletes the stored hash for the given blob
     * @param container Container containing the blob
     * @param key  Key of the blob
     * @return
     */
    CompletableFuture<Void> delete(String container, String key);

	/**
	 * Copies the hash data for one blob to the hash data of another blob
	 * 
	 * @param fromContainer Source container
	 * @param fromKey       Source blob name
	 * @param toContainer   Target container
	 * @param toKey         Target container name
	 * @return {@link CompletableFuture} of the operation.
	 */
	CompletableFuture<Void> copy(String fromContainer, String fromKey, String toContainer, String toKey);


    /**
     * Returns a {@link DataSource} which calculates the hash codes during streaming, and then writes the hash codes to the hash store.
     * @param blob {@link Blob} to get augmented {@link DataSource} for.
     * @return {@link DataSource}.
     */
    DataSource storeHashForOnWrite(Blob blob);
}
