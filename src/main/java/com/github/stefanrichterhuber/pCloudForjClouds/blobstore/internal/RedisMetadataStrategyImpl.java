package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

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
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.PCloudBlobStore;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.DiffResponse.DiffEntry;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.RedisClientProvider;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.lambdaworks.redis.KeyScanCursor;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.ScanArgs;
import com.lambdaworks.redis.ScanCursor;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.RemoteEntry;
import com.pcloud.sdk.RemoteFile;
import com.pcloud.sdk.RemoteFolder;
import com.pcloud.sdk.UploadOptions;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

@Singleton
public class RedisMetadataStrategyImpl implements MetadataStrategy {
    private static final Logger LOGGER = LoggerFactory.getLogger(RedisMetadataStrategyImpl.class);

    private static final String SEPARATOR = "/";

    private final boolean active;

    private final RedisConnection<String, String> redisConnection;
    private static final String REDIS_PREFIX = "PCloudBlobStore:";
    private static final String REDIS_KEY_LAST_DIFF = "PCloudBlobStore-last-diff";

    private final String redisKeyPrefix;

    private final String metadataFolder;
    private final long metadataFolderId;
    private final String baseDirectory;

    private final int sanitizeInterval;
    private final int synchronizeInterval;

    private final ApiClient apiClient;

    @Inject
    protected RedisMetadataStrategyImpl( //
            @Named(PCloudConstants.PROPERTY_USERMETADATA_ACTIVE) boolean active, //
            @Named(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING) String redis, //
            @Named(PCloudConstants.PROPERTY_USERMETADATA_FOLDER) String metadataFolder, //
            @Named(PCloudConstants.PROPERTY_SANITIZE_METADATA_INTERVAL_MIN) int sanitizeInterval, //
            @Named(PCloudConstants.PROPERTY_SYNCHRONIZE_METADATA_INTERVAL_MIN) int synchronizeInterval, //
            @Named(PCloudConstants.PROPERTY_BASEDIR) String baseDir, //
            ApiClient apiClient //
    ) {
        this.active = active;
        this.redisConnection = new RedisClientProvider(redis).get();
        this.apiClient = apiClient;
        this.baseDirectory = baseDir;
        this.metadataFolder = metadataFolder;
        this.metadataFolderId = PCloudUtils.createBaseDirectory(this.apiClient, metadataFolder).folderId();
        this.sanitizeInterval = sanitizeInterval;
        this.synchronizeInterval = synchronizeInterval;

        // We need to distinguish between different users in the cache
        try {
            long userId = apiClient.getUserInfo().execute().userId();
            this.redisKeyPrefix = String.format("%s%d:", REDIS_PREFIX, userId);
        } catch (IOException | ApiError e) {
            throw new RuntimeException(e);
        }

        syncMetadataCache();
    }

    /**
     * Synchronizes the remote metadata folder regularly with the local cache
     */
    private void syncMetadataCache() {
        OkHttpClient httpClient = PCloudUtils.getHTTPClient(apiClient);
        HttpUrl apiHost = HttpUrl.parse("https://" + apiClient.apiHost());
        final Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
                .setDateFormat("EEE, dd MMM yyyy HH:mm:ss zzzz").create();

        final ScheduledExecutorService service = Executors.newScheduledThreadPool(1);

        if (sanitizeInterval > 0) {
            service.scheduleAtFixedRate(() -> this.sanitizeMetadataFiles().join(), sanitizeInterval, sanitizeInterval,
                    TimeUnit.MINUTES);
        }
        if (synchronizeInterval > 0) {
            // Run once immediately, then every 5 minutes or so
            service.scheduleAtFixedRate(() -> this.loadMetadataDiffs(httpClient, apiHost, gson).join(), 0,
                    synchronizeInterval,
                    TimeUnit.MINUTES);

        }

    }

    /**
     * Load all currently available diffs from the pCloud backend since the latest
     * diff referenced in the cache (or all if cache is empty), filter for the ones
     * concerning the metdatafolder and then process these
     * 
     * @param httpClient
     * @param apiHost
     * @param gson
     */
    private CompletableFuture<Void> loadMetadataDiffs(OkHttpClient httpClient, HttpUrl apiHost, Gson gson) {
        final String diffId = this.redisConnection.get(REDIS_KEY_LAST_DIFF);
        return loadMetadataDiffs(diffId, httpClient, apiHost, gson)
                // Ignore errors in the run and hope for the next
                .exceptionally(e -> {
                    LOGGER.warn("Error during fetching metadata: {}", e);
                    return null;
                });
    }

    /**
     * Load all currently available diffs from the pCloud backend since the given
     * diff , filter for the ones
     * concerning the metdatafolder and then process these
     * 
     * @param diffId     Id of the latest diff already processed, might be null to
     *                   process all diffs since start
     * @param httpClient
     * @param apiHost
     * @param gson
     */
    private CompletableFuture<Void> loadMetadataDiffs(String diffId, OkHttpClient httpClient, HttpUrl apiHost,
            Gson gson) {
        HttpUrl.Builder urlBuilder = apiHost.newBuilder() //
                .addPathSegment("diff");
        if (diffId != null) {
            urlBuilder = urlBuilder.addQueryParameter("diffid", diffId);
        }

        final Request request = new Request.Builder().url(urlBuilder.build()).get().build();
        return PCloudUtils.execute(httpClient.newCall(request)).thenComposeAsync(resp -> {
            try (Response response = resp) {
                final JsonReader reader = new JsonReader(
                        new BufferedReader(new InputStreamReader(response.body().byteStream())));
                final DiffResponse r = gson.fromJson(reader, DiffResponse.class);

                final Collection<DiffEntry> applicableEntries = filterForCacheEntries(r.getEntries());
                final String currentDiffId = Long.toString(r.getDiffid());

                return PCloudUtils
                        .allOf(applicableEntries.stream().map(this::applyDiff).collect(Collectors.toList()))
                        .thenCompose(v -> {
                            this.redisConnection.set(REDIS_KEY_LAST_DIFF, currentDiffId);
                            if (currentDiffId.equals(diffId)) {
                                return CompletableFuture.completedFuture(null);
                            } else {
                                // Recursivly try next diffid
                                return loadMetadataDiffs(currentDiffId, httpClient, apiHost, gson);
                            }
                        });
            }
        });
    }

    /**
     * Checks all files in the metadata folder, if the corresponding file still
     * exists
     * If not, the metadatafile and the local cache entry for the file is deleted.
     * 
     * @return
     */
    private CompletableFuture<Void> sanitizeMetadataFiles() {
        return PCloudUtils.execute(this.apiClient.listFolder(metadataFolderId)).thenCompose(rf -> {
            final List<CompletableFuture<Void>> jobs = new ArrayList<>();
            for (RemoteEntry re : rf.children()) {
                // There should be only files here, but just in case
                if (re.isFile()) {
                    jobs.add(sanitizeMetadataFile(re.asFile()));
                } else {
                    LOGGER.warn("There should not be any folders in the metadata folder: {}", re.name());
                }
            }
            return PCloudUtils.allOf(jobs)
                    .exceptionally(e -> {
                        // Ignore error and try again next time.
                        LOGGER.warn("Error during sanitizing metadata: {}", e);
                        return null;
                    })
                    .thenApply(v -> null);
        });
    }

    /**
     * Checks if the file for the given metadata file still exists. If not, the
     * metadata file (and the corresponding cache entry) is deleted.
     * 
     * @param metadataFile {@link RemoteFile} of the metadata file to check
     * @return {@link CompletableFuture} of the async operation.
     */
    private CompletableFuture<Void> sanitizeMetadataFile(RemoteFile metadataFile) {
        return CompletableFuture.supplyAsync(() -> {
            // Read metadata from file
            try {
                ExternalBlobMetadata blobMetadata = ExternalBlobMetadata.readJSON(metadataFile);
                return blobMetadata;
            } catch (IOException e) {
                LOGGER.warn("Ill formed metadata file {}", metadataFile.name());
                throw new RuntimeException(e);
            }
        }).thenCompose(ebm -> {
            // Check if file / folder still exists
            CompletableFuture<? extends RemoteEntry> target = null;

            if (ebm.storageType() == StorageType.BLOB) {
                target = PCloudUtils.execute(this.apiClient.loadFile(ebm.fileId()));
            } else {
                target = PCloudUtils.execute(this.apiClient.loadFolder(ebm.fileId()));
            }

            return target.exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> null)) //
                    .thenCompose(v -> {
                        // If folder / file still exists value is non-null, other wise its null
                        if (v == null) {
                            return PCloudUtils.execute(this.apiClient.deleteFile(metadataFile)).thenApply(s -> {
                                LOGGER.info("Deleted orphaned metadata file {}", metadataFile.name());
                                final String k = getRedisKey(ebm.container(), ebm.key());
                                this.redisConnection.del(k);
                                return null;
                            });
                        } else {
                            // File still exists, everything is fine, just check if it has the correct file
                            // name (Support for future changes of nameing schema)
                            final String canonicalFilename = this.getMetadataFileName(ebm.container(),
                                    ebm.key());
                            if (!metadataFile.name().equals(canonicalFilename)) {
                                LOGGER.info("Renamed metadata file {} to canonical name {}", metadataFile.name(),
                                        canonicalFilename);
                                return PCloudUtils.execute(this.apiClient.rename(metadataFile, canonicalFilename))
                                        .thenApply(r -> null);
                            }
                            return CompletableFuture.completedFuture(null);
                        }
                    });
        });
    }

    /**
     * Takes a {@link List} of {@link DiffEntry}s and collects the ones being file
     * events in the metadata folder. For each file only the latest event is
     * collected.
     * 
     * @param entries {@link DiffEntry}s to filter
     * @return Filtered {@link DiffEntry}s.
     */
    private Collection<DiffEntry> filterForCacheEntries(Collection<DiffEntry> entries) {
        final Set<String> eventsToCheck = Set.of(DiffEntry.EVENT_CREATEFILE, DiffEntry.EVENT_DELETEFILE,
                DiffEntry.EVENT_MODIFYFILE);
        final Map<Long, DiffEntry> applicableEntries = entries.stream()
                .filter(de -> eventsToCheck.contains(de.getEvent()))
                .filter(de -> de.getMetadata() != null && !de.getMetadata().isFolder())
                // Only check for file operations happened in the metadatafolder
                .filter(de -> de.getMetadata().getParentfolderid() == metadataFolderId)
                .collect(Collectors.toMap(
                        v -> v.getMetadata().getFileid(), //
                        v -> v, //
                        (v1, v2) -> v2 // Only use the latest entry for the file
                ));
        return applicableEntries.values();
    }

    /**
     * Load a {@link DiffEntry} to the cache.
     * 
     * @param entry {@link DiffEntry} to apply
     * @return
     */
    private CompletableFuture<Void> applyDiff(DiffEntry entry) {
        if (entry.getEvent().equals(DiffEntry.EVENT_DELETEFILE)) {
            // FIXME: Can be implemented after migration to a decodable file name of
            // metadata files
            return CompletableFuture.completedFuture(null);
        } else if (entry.getEvent().equals(DiffEntry.EVENT_CREATEFILE)
                || entry.getEvent().equals(DiffEntry.EVENT_MODIFYFILE)) {
            return PCloudUtils.execute(this.apiClient.loadFile(entry.getMetadata().getFileid()))
                    .thenAcceptAsync(re -> {
                        try {
                            final ExternalBlobMetadata blobMetadata = ExternalBlobMetadata.readJSON(re);
                            final String k = getRedisKey(blobMetadata.container(), blobMetadata.key());
                            final String v = blobMetadata.toJson();
                            this.redisConnection.set(k, v);
                            LOGGER.debug("Added metadata for blob {}/{} from remote to cache", blobMetadata.container(),
                                    blobMetadata.key());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .exceptionally(e -> {
                        LOGGER.warn("Malformed metadata file {}: {}", entry.getMetadata().getName(), e);
                        return null;
                    });
        } else {
            // Unsupported event type
            return CompletableFuture.completedFuture(null);
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
            return this.getFromCache(container, key)
                    .thenCompose(ebm -> {
                        if (ebm != null) {
                            // Data found in local cache
                            return CompletableFuture.completedFuture(ebm);
                        } else {
                            // Data not found in local cache, try remote metadata folder
                            return this.getFromMetadataFolder(container, key);
                        }
                    }) //
                    .thenCompose(ebm -> {
                        if (ebm != null) {
                            // Data found in remote metadatafolder,
                            return CompletableFuture.completedFuture(ebm);
                        } else {
                            // Data not found in remote folder, restore from file content
                            return this.getFromFile(container, key);
                        }
                    });
        } else {
            return CompletableFuture.completedFuture(ExternalBlobMetadata.EMPTY_METADATA);
        }
    }

    /**
     * Tries to find the metadata for blob in the redis cache
     * 
     * @param container Container of the blob
     * @param key       Key of the blob
     * @return
     */
    private CompletableFuture<ExternalBlobMetadata> getFromCache(String container, String key) {
        return CompletableFuture.supplyAsync(() -> {
            String k = getRedisKey(container, key);

            String v = this.redisConnection.get(k);

            if (v != null) {
                ExternalBlobMetadata md = ExternalBlobMetadata.fromJSON(v);
                LOGGER.debug("Found metadata for {}/{} in cache-> {}", container, key, md);
                return md;
            } else {
                LOGGER.debug("No metadata found for {}/{} in cache", container, key);
                return null;
            }
        });
    }

    /**
     * Gets the name of the file to store metadata on the backend.
     * 
     * @param container Container of the blob
     * @param key       Key of the blob
     * @return
     */
    @SuppressWarnings("deprecation")
    private String getMetadataFileName(String container, String key) {
        // TODO change to base64 of filename or something alike with clear text
        // container name
        return "BLOB" + Hashing.md5().hashString(container + SEPARATOR + key, StandardCharsets.UTF_8).toString()
                + ".json";
    }

    /**
     * Reads the metadata file from the {@link #metadataFolder}
     * 
     * @param container Container of the blob
     * @param key       Key of the blob
     * @return {@link ExternalBlobMetadata} (if found)
     */
    private CompletableFuture<ExternalBlobMetadata> getFromMetadataFolder(String container, String key) {
        String filename = getMetadataFileName(container, key);
        return PCloudUtils.execute(this.apiClient.loadFile(metadataFolder + SEPARATOR + filename))
                .thenApplyAsync(rf -> {
                    try {
                        final ExternalBlobMetadata blobMetadata = ExternalBlobMetadata.readJSON(rf);
                        final String k = getRedisKey(container, key);
                        final String v = blobMetadata.toJson();
                        this.redisConnection.set(k, v);
                        return blobMetadata;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }).exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> null));
    }

    /**
     * Creates a PCloud path to the target object starting with the
     * {@link #baseDirectory}.
     * 
     * @param content Path parts
     * @return Path
     */
    private String createPath(String... content) {
        if (content != null && content.length > 0) {
            return this.baseDirectory + SEPARATOR
                    + Arrays.asList(content).stream().collect(Collectors.joining(SEPARATOR));
        } else {
            return this.baseDirectory;
        }
    }

    /**
     * If no metadata is found, check if the file exists at all, and create some
     * barebones metadata for it
     * 
     * @param container Container of the blob
     * @param key       Key of the blob
     * @return {@link ExternalBlobMetadata} (if found)
     */
    private CompletableFuture<ExternalBlobMetadata> getFromFile(String container, String key) {
        String filename = createPath(container, key);
        if (filename.endsWith(SEPARATOR)) {
            // This is a folder blob
            return PCloudUtils.execute(this.apiClient.loadFolder(PCloudBlobStore.stripDirectorySuffix(filename)))
                    .thenCompose(
                            rf -> this.restoreMetadata(container, key, BlobAccess.PRIVATE, Collections.emptyMap(), rf))
                    .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> null));
        } else {
            // This is a file blob
            return PCloudUtils.execute(this.apiClient.loadFile(filename))
                    .thenCompose(
                            rf -> this.restoreMetadata(container, key, BlobAccess.PRIVATE, Collections.emptyMap(), rf))
                    .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> null));
        }
    }

    @Override
    public CompletableFuture<Void> put(String container, String key, ExternalBlobMetadata metadata) {
        if (active) {
            if (metadata != null) {
                final String filename = getMetadataFileName(container, key);
                final String json = metadata.toJson();
                return PCloudUtils
                        .execute(this.apiClient.createFile(metadataFolderId, filename,
                                DataSource.create(json.getBytes(StandardCharsets.UTF_8)), UploadOptions.OVERRIDE_FILE))
                        .thenApplyAsync(rf -> {
                            // File uploaded, now set cache
                            final String k = getRedisKey(container, key);
                            this.redisConnection.set(k, json);
                            return null;
                        });
            } else {
                return delete(container, key);
            }
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<Void> delete(String container, String key) {
        if (active) {
            final String filename = getMetadataFileName(container, key);

            return PCloudUtils.execute(this.apiClient.deleteFile(filename)).thenApplyAsync(s -> {
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
     * @param container    COntainer of the blob
     * @param key          Key of the blob
     * @param blobAccess   {@link BlobAccess} to set in metadata
     * @param usermetadata Additional user metadata
     * @param entry        {@link RemoteEntry} to generate the metadata for.
     * @return {@link ExternalBlobMetadata} generated and stored in cache.
     */
    public CompletableFuture<ExternalBlobMetadata> restoreMetadata(String container, String key, BlobAccess blobAccess,
            Map<String, String> usermetadata,
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
                                        blobAccess, blobHashes.withBuildin(file.hash()), usermetadata)

                        )
                        .thenCompose(metadata -> this.put(container, key, metadata).thenApply(v -> metadata));
            } else {
                final RemoteFolder folder = entry.asFolder();
                final ExternalBlobMetadata metadata = new ExternalBlobMetadata(container, key, folder.folderId(),
                        StorageType.FOLDER, blobAccess, BlobHashes.empty(), usermetadata);

                return this.put(container, key, metadata).thenApply(v -> metadata);
            }
        } else {
            return CompletableFuture.completedFuture(ExternalBlobMetadata.EMPTY_METADATA);
        }
    }

}
