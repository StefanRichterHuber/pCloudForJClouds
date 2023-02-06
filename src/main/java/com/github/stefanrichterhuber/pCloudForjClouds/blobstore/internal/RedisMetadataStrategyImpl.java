package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;

import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.BlobHashes;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.ExternalBlobMetadata;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.MetadataStrategy;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.RedisClientProvider;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.lambdaworks.redis.KeyScanCursor;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.ScanArgs;
import com.lambdaworks.redis.ScanCursor;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.RemoteEntry;
import com.pcloud.sdk.RemoteFile;
import com.pcloud.sdk.RemoteFolder;

public class RedisMetadataStrategyImpl implements MetadataStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMetadataStrategyImpl.class);

    private static final String SEPARATOR = "/";

    private final boolean active;

    private final RedisConnection<String, String> redisConnection;
    private static final String REDIS_PREFIX = "PCloudBlobStore:";

    private final String redisKeyPrefix;

    private final ApiClient apiClient;

    @Inject
    protected RedisMetadataStrategyImpl( //
            @Named(PCloudConstants.PROPERTY_USERMETADATA_ACTIVE) boolean active, //
            @Named(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING) String redis, //
            ApiClient apiClient //
    ) {
        this.active = active;
        this.redisConnection = new RedisClientProvider(redis).get();
        this.apiClient = apiClient;

        // We need to distinguish between different users in the cache
        try {
            long userId = apiClient.getUserInfo().execute().userId();
            this.redisKeyPrefix = String.format("%s%d:", REDIS_PREFIX, userId);
        } catch (IOException | ApiError e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the key to store blob metadata in the REDIS cache
     * 
     * @param container Container of the blob
     * @param key       Key of the blob
     * @return
     */
    private String getRedisKey(String container, String key) {
        return redisKeyPrefix + container + SEPARATOR + key;
    }

    @Override
    public CompletableFuture<ExternalBlobMetadata> get(String container, String key) {
        if (active) {
            return CompletableFuture.supplyAsync(() -> {
                String k = getRedisKey(container, key);

                String v = this.redisConnection.get(k);

                if (v != null) {
                    ExternalBlobMetadata md = ExternalBlobMetadata.fromJSON(v);
                    LOGGER.debug("Found metadata for {}/{} -> {}", container, key, md);
                    return md;
                } else {
                    LOGGER.debug("No metadata found for {}/{}", container, key);
                    return null;
                }
            });
        } else {
            return CompletableFuture.completedFuture(ExternalBlobMetadata.EMPTY_METADATA);
        }
    }

    @Override
    public CompletableFuture<Void> put(String container, String key, ExternalBlobMetadata metadata) {
        if (active) {
            return CompletableFuture.supplyAsync(() -> {
                final String k = getRedisKey(container, key);

                if (metadata != null) {
                    final String v = metadata.toJson();
                    LOGGER.debug("Set metadata {} for {}/{}", metadata, container, key);
                    this.redisConnection.set(k, v);
                } else {
                    this.redisConnection.del(k);
                }
                return null;
            });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<Void> delete(String container, String key) {
        if (active) {
            return CompletableFuture.supplyAsync(() -> {
                final String k = getRedisKey(container, key);
                LOGGER.debug("Delete metadata for {}/{}", container, key);
                this.redisConnection.del(k);
                return null;
            });
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<PageSet<ExternalBlobMetadata>> list(String containerName,
            ListContainerOptions options) {
        if (active) {
            LOGGER.debug("List metadata for container {} with options {}", containerName, options);

            if (options == null) {
                return list(containerName, ListContainerOptions.NONE);
            }

            // number of results to fetch
            int resultsToFetch = options.getMaxResults() != null ? options.getMaxResults() : 1000;

            String match = redisKeyPrefix + containerName + SEPARATOR;
            if (options.getPrefix() != null) {
                match += options.getPrefix();
            }
            match += "*";

            int batchSize = Math.min(resultsToFetch, 50);
            final ScanArgs scanArgs = ScanArgs.Builder.limit(batchSize).match(match);

            KeyScanCursor<String> cursor = options != null && options.getMarker() != null
                    ? this.redisConnection.scan(ScanCursor.of(options.getMarker()), scanArgs)
                    : this.redisConnection.scan(scanArgs);

            String nextMarker = null;
            final List<CompletableFuture<ExternalBlobMetadata>> jobs = new ArrayList<>(resultsToFetch);

            List<String> keys = filterCacheKeysForListOptions(cursor.getKeys(), options, containerName);

            jobs.addAll(loadMetadata(keys));
            nextMarker = cursor.getCursor();
            resultsToFetch -= keys.size();

            while (!cursor.isFinished() && resultsToFetch > 0) {
                batchSize = Math.min(resultsToFetch, 50);
                cursor = this.redisConnection.scan(cursor, ScanArgs.Builder.limit(batchSize).match(match));
                keys = filterCacheKeysForListOptions(cursor.getKeys(), options, containerName);
                jobs.addAll(loadMetadata(keys));
                nextMarker = cursor.getCursor();
                resultsToFetch -= keys.size();
            }

            final String marker = cursor.isFinished() ? null : nextMarker;

            final CompletableFuture<PageSet<ExternalBlobMetadata>> result = PCloudUtils.allOf(jobs, TreeSet::new)
                    .thenApply(contents -> {
                        // trim metadata, if the response isn't supposed to be detailed.
                        if (!options.isDetailed()) {
                            for (ExternalBlobMetadata md : contents) {
                                md.customMetadata().clear();
                            }
                        }
                        return new PageSetImpl<ExternalBlobMetadata>(contents, marker);
                    });
            return result;
        } else {
            return CompletableFuture.completedFuture(new PageSetImpl<>(Collections.emptyList(), null));
        }
    }

    /**
     * Loads all the {@link ExternalBlobMetadata} for the {@link List} of metadata
     * keys
     * 
     * @param keys Cache keys
     */
    private List<CompletableFuture<ExternalBlobMetadata>> loadMetadata(List<String> keys) {
        final List<CompletableFuture<ExternalBlobMetadata>> jobs = new ArrayList<>(keys.size());
        // process data and fetch the file metadata for each job sync
        for (String key : keys) {
            final CompletableFuture<ExternalBlobMetadata> storageMetadata = CompletableFuture.supplyAsync(() -> {
                final String v = this.redisConnection.get(key);
                final ExternalBlobMetadata md = ExternalBlobMetadata.fromJSON(v);
                return md;
            });
            jobs.add(storageMetadata);
        }
        return jobs;
    }

    /**
     * Filter out cache keys not matching the given {@link ListContainerOptions}
     * 
     * @param keys    {@link List} of keys to filter
     * @param options {@link ListContainerOptions} to apply. Might be null or
     *                {@link ListContainerOptions#NONE}
     * @return filtered {@link List}
     */
    private List<String> filterCacheKeysForListOptions(List<String> keys, ListContainerOptions options,
            String container) {
        if (keys == null || options == null) {
            return keys;
        }

        if (!options.isRecursive()) {
            final int prefixLength = ((redisKeyPrefix + container + SEPARATOR)
                    + (options.getPrefix() != null ? options.getPrefix() : "")).length();

            keys = keys.stream().filter(key -> {
                final String subKey = key.substring(prefixLength);
                return !subKey.contains(SEPARATOR);
            }).collect(Collectors.toList());
        }

        return keys;
    }

    /**
     * Restores the metadata entries (without custom metadata!) for the given
     * {@link RemoteFile}. Data is stored in cache.
     * 
     * @param container  COntainer of the blob
     * @param key        Key of the blob
     * @param blobAccess {@link BlobAccess} to set in metadata
     * @param entry      {@link RemoteEntry} to generate the metadata for.
     * @return {@link ExternalBlobMetadata} generated and stored in cache.
     */
    public CompletableFuture<ExternalBlobMetadata> restoreMetadata(String container, String key, BlobAccess blobAccess,
            RemoteEntry entry) {
        if (active) {
            if (entry.isFile()) {
                final RemoteFile file = entry.asFile();
                return PCloudUtils.calculateChecksum(apiClient, file.fileId())
                        .thenApplyAsync(blobHashes -> {
                            // european pCloud locations do not deliver MD5 checksums -> so calculate
                            // manually (although very expensive :/)
                            if (blobHashes.md5() == null) {
                                try {
                                    return BlobHashes.from(file);
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            } else {
                                return blobHashes;
                            }
                        })
                        .thenApply(
                                blobHashes -> new ExternalBlobMetadata(container, key, file.fileId(), StorageType.BLOB,
                                        blobAccess, blobHashes.withBuildin(file.hash()), Collections.emptyMap())

                        )
                        .thenCompose(metadata -> this.put(container, key, metadata).thenApply(v -> metadata));
            } else {
                final RemoteFolder folder = entry.asFolder();
                final ExternalBlobMetadata metadata = new ExternalBlobMetadata(container, key, folder.folderId(),
                        StorageType.FOLDER, blobAccess, BlobHashes.empty(), Collections.emptyMap());

                return this.put(container, key, metadata).thenApply(v -> metadata);
            }
        } else {
            return CompletableFuture.completedFuture(ExternalBlobMetadata.EMPTY_METADATA);
        }
    }

}
