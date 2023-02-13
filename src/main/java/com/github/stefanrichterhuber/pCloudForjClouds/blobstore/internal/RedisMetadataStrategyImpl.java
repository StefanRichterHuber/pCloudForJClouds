package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.BlobHashes;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.ExternalBlobMetadata;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.MetadataStrategy;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.PCloudBlobStore;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.DiffResponse.DiffEntry;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
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

    private static final String REDIS_PREFIX = "PCloudBlobStore:";
    private static final String REDIS_KEY_LAST_DIFF_PREFIX = "last-diff.";

    private final String redisKeyPrefix;
    private final String redisLastDiffKey;

    private final String metadataFolder;
    private final long metadataFolderId;
    private final String baseDirectory;

    private final int sanitizeInterval;
    private final int synchronizeInterval;

    private final ApiClient apiClient;

    private final Executor backgroundExecutor;
    private final RedissonClient redisson;
    private final RMap<String, String> redisCache;

    @Inject
    protected RedisMetadataStrategyImpl( //
            @Named(PCloudConstants.PROPERTY_USERMETADATA_ACTIVE) boolean active, //
            @Named(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING) String redis, //
            @Named(PCloudConstants.PROPERTY_USERMETADATA_FOLDER) String metadataFolder, //
            @Named(PCloudConstants.PROPERTY_SANITIZE_METADATA_INTERVAL_MIN) int sanitizeInterval, //
            @Named(PCloudConstants.PROPERTY_SYNCHRONIZE_METADATA_INTERVAL_MIN) int synchronizeInterval, //
            @Named(PCloudConstants.PROPERTY_BASEDIR) String baseDir, //
            RedissonClient redisson,
            ApiClient apiClient //
    ) {
        this.active = active;
        this.apiClient = apiClient;
        this.baseDirectory = baseDir;
        this.metadataFolder = metadataFolder;
        this.metadataFolderId = PCloudUtils.createBaseDirectory(this.apiClient, metadataFolder).folderId();
        this.sanitizeInterval = sanitizeInterval;
        this.synchronizeInterval = synchronizeInterval;
        this.redisson = redisson;
        this.redisCache = this.redisson.getMap(REDIS_PREFIX, StringCodec.INSTANCE);

        this.backgroundExecutor = Executors.newCachedThreadPool();

        // We need to distinguish between different users in the cache
        try {
            final long userId = apiClient.getUserInfo().execute().userId();

            this.redisKeyPrefix = String.format("%d:", userId);
            this.redisLastDiffKey = String.format("%s%d:", REDIS_KEY_LAST_DIFF_PREFIX, userId);
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
        if (sanitizeInterval == 0) {
            // Schedule the sanitize job
            service.schedule(() -> sanitizeMetadataFiles().join(), 5, TimeUnit.MINUTES);
        }
        if (synchronizeInterval > 0) {
            // Run once immediately, then every 5 minutes or so
            service.scheduleAtFixedRate(() -> this.loadMetadataDiffs(httpClient, apiHost, gson).join(), 0,
                    synchronizeInterval,
                    TimeUnit.MINUTES);

        }
        if (synchronizeInterval == 0) {
            service.schedule(() -> this.loadMetadataDiffs(httpClient, apiHost, gson).join(), 0, TimeUnit.MINUTES);
        }

    }

    /**
     * Load all currently available diffs from the pCloud backend since the latest
     * diff referenced in the cache (or all if cache is empty), filter for the ones
     * concerning the metadatafolder and then process these
     * 
     * @param httpClient
     * @param apiHost
     * @param gson
     */
    private CompletableFuture<Void> loadMetadataDiffs(OkHttpClient httpClient, HttpUrl apiHost, Gson gson) {
        final String diffId = this.redisCache.get(redisLastDiffKey);
        LOGGER.info("Start syncing metadata files since {}", diffId);
        return loadMetadataDiffs(diffId, httpClient, apiHost, gson)
                // Ignore errors in the run and hope for the next
                .exceptionally(e -> {
                    LOGGER.warn("Error during fetching metadata: {}", e);
                    return null;
                })
                .thenAccept(v -> {
                    LOGGER.info("Finished syncing metadata files");
                });
    }

    /**
     * Load all currently available diffs from the pCloud backend since the given
     * diff , filter for the ones
     * concerning the metadatafolder and then process these
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
                        .thenComposeAsync(v -> {

                            this.redisCache.put(redisLastDiffKey, currentDiffId);
                            if (currentDiffId.equals(diffId)) {
                                return CompletableFuture.completedFuture(null);
                            } else {
                                // Recursivly try next diffid
                                return loadMetadataDiffs(currentDiffId, httpClient, apiHost, gson);
                            }
                        }, backgroundExecutor);
            }
        });
    }

    /**
     * Takes a {@link Collection} of {@link DiffEntry}s and collects the ones being
     * file
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
            // FIXME: Can be implemented after migration to a decodeable file name of
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
                            this.redisCache.put(k, v);
                            LOGGER.debug("Added metadata for blob {}/{} from remote to cache", blobMetadata.container(),
                                    blobMetadata.key());
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }, backgroundExecutor)
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
     * Checks all files in the metadata folder, if the corresponding file still
     * exists
     * If not, the metadatafile and the local cache entry for the file is deleted.
     * 
     * @return
     */
    private CompletableFuture<Void> sanitizeMetadataFiles() {
        LOGGER.info("Start sanitizing metadatafiles");
        return PCloudUtils.execute(this.apiClient.listFolder(metadataFolderId)).thenComposeAsync(rf -> {
            final List<CompletableFuture<Void>> jobs = rf.children().stream().filter(RemoteEntry::isFile)
                    .map(RemoteEntry::asFile).map(this::sanitizeMetadataFile).collect(Collectors.toList());
            return PCloudUtils.allOf(jobs)
                    .exceptionally(e -> {
                        // Ignore error and try again next time.
                        LOGGER.warn("Error during sanitizing metadata: {}", e);
                        return null;
                    })
                    .thenApply(v -> null);
        }, backgroundExecutor).thenAccept(v -> {
            LOGGER.info("Finished sanitizing metadatafiles");
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
        return CompletableFuture
                .supplyAsync(() -> {
                    // Read metadata from file
                    try {
                        ExternalBlobMetadata blobMetadata = ExternalBlobMetadata.readJSON(metadataFile);
                        return blobMetadata;
                    } catch (IOException e) {
                        LOGGER.warn("Ill formed metadata file {}", metadataFile.name());
                        throw new RuntimeException(e);
                    }
                }, backgroundExecutor)
                .thenComposeAsync(ebm -> {
                    // Check if file / folder still exists
                    final CompletableFuture<? extends RemoteEntry> target = ebm.storageType() == StorageType.BLOB
                            ? PCloudUtils.execute(this.apiClient.loadFile(ebm.fileId()))
                            : PCloudUtils.execute(this.apiClient.loadFolder(ebm.fileId()));

                    return target
                            .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> null)) //
                            .thenComposeAsync(v -> {
                                // If folder / file still exists value is non-null, other wise its null
                                if (v == null) {
                                    return PCloudUtils.execute(this.apiClient.deleteFile(metadataFile))
                                            .thenApplyAsync(s -> {
                                                LOGGER.info("Deleted orphaned metadata file {}", metadataFile.name());
                                                final String k = getRedisKey(ebm.container(), ebm.key());
                                                this.redisCache.remove(k);
                                                return null;
                                            }, backgroundExecutor);
                                } else {
                                    // File still exists, everything is fine, just check if it has the correct file
                                    // name (Support for future changes of naming schema)
                                    final String canonicalFilename = this.getMetadataFileName(ebm.container(),
                                            ebm.key());
                                    if (!metadataFile.name().equals(canonicalFilename)) {
                                        LOGGER.info("Renamed metadata file {} to canonical name {}",
                                                metadataFile.name(),
                                                canonicalFilename);
                                        return PCloudUtils
                                                .execute(this.apiClient.rename(metadataFile, canonicalFilename))
                                                .thenApply(r -> null);
                                    }
                                    return CompletableFuture.completedFuture(null);
                                }
                            }, backgroundExecutor);
                }, backgroundExecutor);
    }

    /**
     * Returns the key to store blob metadata in the REDIS cache
     * 
     * @param container Container of the blob
     * @param key       Key of the blob
     * @return
     */
    private String getRedisKey(String container, String key) {
        if (key == null) {
            return container;
        }
        return container + SEPARATOR + key;
    }

    /**
     * Checks if the target value sits in the cache, if not, lock the cache for the
     * key and look up either the remote metadata folder or create the metadata for
     * an existing file or use the factory to create the metadata.
     */
    @Override
    public CompletableFuture<ExternalBlobMetadata> getOrCreate(String container, String key,
            BiFunction<String, String, CompletableFuture<ExternalBlobMetadata>> factory) {

        return this.getFromCache(container, key)
                .thenCompose(olm -> {
                    if (olm != null) {
                        return CompletableFuture.completedFuture(olm);
                    } else {
                        final String k = getRedisKey(container, key);
                        final RLock lock = this.redisCache.getLock(k);
                        final long threadId = Thread.currentThread().getId();

                        return lock.lockAsync(threadId)
                                .thenApply(v -> {
                                    LOGGER.trace("Locked metadata entry {}/{} for thread {}", container, key, threadId);
                                    return v;
                                })
                                .thenCompose(v -> this.getOrCreateWithoutLock(container, key, factory))
                                .thenCompose(v -> lock.unlockAsync(threadId).thenApply(x -> {
                                    LOGGER.trace("Unlocked metadata entry {}/{} for thread {}", container, key,
                                            threadId);
                                    return v;
                                }))
                                .exceptionallyCompose(
                                        e -> lock.unlockAsync(threadId)
                                                .thenCompose(v -> CompletableFuture.failedStage(e)))
                                .toCompletableFuture();
                    }
                });

    }

    /**
     * Checks if the value exists, or creates if if not using the given factory.
     * This variant of the method does not perform a lock, so it could be used by
     * method with prior lock.
     * 
     * @param container Container of the blob
     * @param key       Key of the blob
     * @param factory   Factory to create the metadata for the lob
     * @return {@link ExternalBlobMetadata} found / created
     */
    private CompletableFuture<ExternalBlobMetadata> getOrCreateWithoutLock(String container, String key,
            BiFunction<String, String, CompletableFuture<ExternalBlobMetadata>> factory) {
        return this.getFromCache(container, key)
                .thenCompose(ebm -> {
                    if (ebm != null) {
                        // Data found in local cache
                        LOGGER.debug("Determined value {} for key {}/{} from cache", ebm, container, key);
                        return CompletableFuture.completedFuture(ebm);
                    } else {
                        LOGGER.debug("Metadata for {}/{} not found locally ", container, key);
                        // Data not found in local cache, try remote metadata folder
                        return this.getFromRemote(container, key)
                                .thenCompose(em -> {
                                    if (em != null) {
                                        return CompletableFuture.completedFuture(em);
                                    } else {
                                        if (factory != null) {
                                            return factory.apply(container, key)
                                                    .thenApply(emn -> {
                                                        LOGGER.info("Determined value {} for {}/{} from factory", emn,
                                                                container, key);
                                                        return emn;
                                                    })
                                                    .thenCompose(emn -> emn != null
                                                            ? this.putWithoutLock(container, key, emn)
                                                                    .thenApply(v -> emn)
                                                            : CompletableFuture.completedFuture(emn))
                                                    .exceptionallyCompose(e -> {
                                                        ApiError err = null;
                                                        if (e instanceof ApiError) {
                                                            err = (ApiError) e;
                                                        } else if (e.getCause() instanceof ApiError) {
                                                            err = (ApiError) e.getCause();
                                                        } else if (e.getCause() != null
                                                                && e.getCause().getCause() instanceof ApiError) {
                                                            err = (ApiError) e.getCause().getCause();
                                                        }
                                                        if (err != null && err.errorCode() == PCloudError.ALREADY_EXISTS
                                                                .getCode()) {
                                                            LOGGER.info(
                                                                    "Requested to generate object {}/{} but it already exists",
                                                                    container, key);
                                                            return getOrCreateWithoutLock(container, key, null);
                                                        }
                                                        return CompletableFuture.failedStage(e);
                                                    });
                                        }
                                        return CompletableFuture.completedFuture(em);
                                    }
                                })
                                .toCompletableFuture();
                    }
                });

    }

    @Override
    public CompletableFuture<ExternalBlobMetadata> get(String container, String key) {
        return getOrCreate(container, key, null);
    }

    /**
     * Tries to find the requested {@link ExternalBlobMetadata} in either the remote
     * metadata folder, or create it from the existing blob
     * 
     * @param container Container of the blob
     * @param key       Key of the blob
     * @return {@link ExternalBlobMetadata} found
     */
    private CompletableFuture<ExternalBlobMetadata> getFromRemote(String container, String key) {
        if (active) {
            // Data not found in local cache, try remote metadata folder
            return this.getFromMetadataFolder(container, key)
                    .thenCompose(em -> this.putWithoutLock(container, key, em)
                            .thenApply(v -> em))
                    .thenCompose(ebm -> {
                        if (ebm != null) {
                            // Data found in remote metadata folder,
                            return CompletableFuture.completedFuture(ebm);
                        } else {
                            // Data not found in remote folder, restore from file content
                            return this.restoreFromFile(container, key);
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
        final String k = getRedisKey(container, key);
        return this.redisCache.getAsync(k)
                .thenApply(v -> {
                    if (v != null) {
                        final ExternalBlobMetadata md = ExternalBlobMetadata.fromJSON(v);
                        LOGGER.debug("Found metadata for {}/{} in cache-> {}", container, key, md);
                        return md;
                    } else {
                        LOGGER.debug("No metadata found for {}/{} in cache", container, key);
                        return null;
                    }
                }).toCompletableFuture();

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
        if (key == null) {
            return "CONTAINER." + container + ".json";
        }
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
                    + Arrays.asList(content).stream().filter(c -> c != null).collect(Collectors.joining(SEPARATOR));
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
    private CompletableFuture<ExternalBlobMetadata> restoreFromFile(String container, String key) {
        String filename = createPath(container, key);
        if (key == null) {
            // This is a container blob
            return PCloudUtils.execute(this.apiClient.loadFolder(filename))
                    .thenComposeAsync(
                            rf -> this.restoreMetadataWithoutLock(container,
                                    key, BlobAccess.PRIVATE, Collections.emptyMap(), rf))
                    .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> null));
        }
        if (filename.endsWith(SEPARATOR)) {
            // This is definitly a folder blob
            return PCloudUtils.execute(this.apiClient.loadFolder(PCloudBlobStore.stripDirectorySuffix(filename)))
                    .thenComposeAsync(
                            rf -> this.restoreMetadataWithoutLock(container, key, BlobAccess.PRIVATE,
                                    Collections.emptyMap(), rf))
                    .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> null));
        } else {
            // This is a file blob
            return PCloudUtils.execute(this.apiClient.loadFile(filename))
                    .thenCompose(rf -> {
                        if (rf.isFolder()) {
                            // It was not a file, but is a folder :/
                            return PCloudUtils
                                    .execute(this.apiClient.loadFolder(PCloudBlobStore.stripDirectorySuffix(filename)))
                                    .thenApply(f -> (RemoteEntry) f);
                        }
                        return CompletableFuture.completedFuture(rf);
                    })
                    .thenComposeAsync(
                            rf -> this.restoreMetadataWithoutLock(container, key, BlobAccess.PRIVATE,
                                    Collections.emptyMap(), rf))
                    .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> null));
        }
    }

    @Override
    public CompletableFuture<Void> put(String container, String key, ExternalBlobMetadata metadata) {
        final String k = getRedisKey(container, key);
        final RLock lock = this.redisCache.getLock(k);
        final long threadId = Thread.currentThread().getId();

        return lock.lockAsync(threadId)
                .thenCompose(v -> this.putWithoutLock(container, key, metadata))
                .thenCompose(v -> lock.unlockAsync(threadId))
                .exceptionallyCompose(
                        e -> lock.unlockAsync(threadId).thenCompose(v -> CompletableFuture.failedStage(e)))
                .toCompletableFuture();
    }

    /**
     * This variant of the method does not perform a lock, so it could be used by
     * method with prior lock.
     * 
     * @param container
     * @param key
     * @param metadata
     * @return
     */
    private CompletableFuture<Void> putWithoutLock(String container, String key, ExternalBlobMetadata metadata) {
        if (active) {
            if (metadata != null) {
                final String filename = getMetadataFileName(container, key);
                final String json = metadata.toJson();
                final String k = getRedisKey(container, key);

                // Both operations can be done in parallel
                final CompletableFuture<RemoteFile> fileOp = PCloudUtils
                        .execute(this.apiClient.createFile(metadataFolderId, filename,
                                DataSource.create(json.getBytes(StandardCharsets.UTF_8)), UploadOptions.OVERRIDE_FILE));
                final CompletableFuture<Void> cacheOp = this.redisCache.fastPutAsync(k, json)
                        .thenApply(v -> (Void) null)
                        .toCompletableFuture();

                // Execute both operations ins parallel
                return CompletableFuture.allOf(fileOp, cacheOp).exceptionallyCompose(e -> {
                    // Clean up on errors
                    return this.deleteWithoutLock(container, key).thenCompose(v -> CompletableFuture.failedStage(e));
                }).exceptionallyCompose(e -> {
                    LOGGER.error("Failed to upload metadata for {}/{}: {}", container, key, e);
                    return CompletableFuture.failedFuture(e);
                });
            } else {
                return deleteWithoutLock(container, key);
            }
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    @Override
    public CompletableFuture<Void> delete(String container, String key) {
        final String k = getRedisKey(container, key);
        final RLock lock = this.redisCache.getLock(k);
        final long threadId = Thread.currentThread().getId();

        return lock.lockAsync(threadId)
                .thenCompose(v -> this.deleteWithoutLock(container, key))
                .thenCompose(v -> lock.unlockAsync(threadId))
                .exceptionallyCompose(
                        e -> lock.unlockAsync(threadId).thenCompose(v -> CompletableFuture.failedStage(e)))
                .toCompletableFuture();
    }

    /**
     * This variant of the method does not perform a lock, so it could be used by
     * method with prior lock.
     * 
     * @param container
     * @param key
     * @return
     */
    private CompletableFuture<Void> deleteWithoutLock(String container, String key) {
        if (active) {
            final String filename = this.metadataFolder + SEPARATOR + getMetadataFileName(container, key);
            final String k = getRedisKey(container, key);

            // Both operations can be done in parallel
            final CompletableFuture<Boolean> fileOp = PCloudUtils.execute(this.apiClient.deleteFile(filename))
                    .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> true));
            final CompletableFuture<Boolean> cacheOp = redisCache.fastRemoveAsync(k)
                    .thenApply(v -> true)
                    .toCompletableFuture();

            return CompletableFuture.allOf(fileOp, cacheOp).thenAccept(v -> {
                if (fileOp.join() && cacheOp.join()) {
                    LOGGER.debug("Deleted metadata for {}/{}", container, key);
                } else {
                    LOGGER.warn("Failed to delete metadata for {}/{}", container, key);
                }
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
            final int resultsToFetch = options.getMaxResults() != null ? options.getMaxResults() : 1000;
            final String match = containerName + SEPARATOR + (options.getPrefix() != null ? options.getPrefix() : "");
            final String marker = options.getMarker();

            return this.redisCache.readAllKeySetAsync()
                    .thenApply(keys -> keys.stream()
                            .filter(k -> k.startsWith(match))
                            .sorted()
                            .dropWhile(e -> marker == null ? false : e.compareTo(marker) <= 0)
                            .filter(e -> matchesListOptions(e, options, containerName)) //
                            .filter(e -> e != null) //
                            .limit(resultsToFetch) //
                            .collect(Collectors.toCollection(TreeSet::new)))
                    .thenCompose(keys -> {
                        final String nextMarker = keys.isEmpty() || keys.size() < resultsToFetch ? null
                                : keys.last();

                        return PCloudUtils.allOf(keys.stream()
                                .map(k -> this.redisCache.getAsync(k).thenApply(v -> ExternalBlobMetadata.fromJSON(v))
                                        .toCompletableFuture())
                                .collect(Collectors.toList()))
                                .thenApply(l -> {
                                    var r = l.stream().filter(e -> e != null).peek(e -> {
                                        if (!options.isDetailed()) {
                                            e.customMetadata().clear();
                                        }
                                    }).collect(Collectors.toCollection(TreeSet::new));
                                    return r;
                                })
                                .thenApply(
                                        contents -> (PageSet<ExternalBlobMetadata>) new PageSetImpl<ExternalBlobMetadata>(
                                                contents, nextMarker));
                    })
                    .toCompletableFuture();
        } else {
            return CompletableFuture.completedFuture(new PageSetImpl<>(Collections.emptyList(), null));
        }
    }

    /**
     * Filter out cache keys not matching the given {@link ListContainerOptions}
     * 
     * @param keys    {@link List} of keys to filter
     * @param options {@link ListContainerOptions} to apply. Might be null or
     *                {@link ListContainerOptions#NONE}
     * @return filtered {@link List}
     */
    private boolean matchesListOptions(String key, ListContainerOptions options,
            String container) {
        if (key == null || options == null) {
            return true;
        }

        if (!options.isRecursive()) {
            final int prefixLength = (container + SEPARATOR
                    + (options.getPrefix() != null ? options.getPrefix() : "")).length();

            final String subKey = key.substring(prefixLength);
            return !subKey.contains(SEPARATOR);
        }
        return true;
    }

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
    public CompletableFuture<ExternalBlobMetadata> restoreMetadata(String container, String key, BlobAccess blobAccess,
            Map<String, String> usermetadata,
            RemoteEntry entry) {

        final String k = getRedisKey(container, key);
        final RLock lock = this.redisCache.getLock(k);
        final long threadId = Thread.currentThread().getId();

        return lock.lockAsync(threadId)
                .thenCompose(v -> restoreMetadataWithoutLock(container, key, blobAccess, usermetadata, entry))
                .thenCompose(v -> lock.unlockAsync(threadId).thenApply(x -> v))
                .exceptionallyCompose(
                        e -> lock.unlockAsync(threadId).thenCompose(v -> CompletableFuture.failedStage(e)))
                .toCompletableFuture();
    }

    /**
     * This variant of the method does not perform a lock, so it could be used by
     * method with prior lock.
     * 
     * @param container
     * @param key
     * @param blobAccess
     * @param usermetadata
     * @param entry
     * @return
     */
    private CompletableFuture<ExternalBlobMetadata> restoreMetadataWithoutLock(String container, String key,
            BlobAccess blobAccess,
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
                        .thenCompose(metadata -> this.putWithoutLock(container, key, metadata)
                                .thenApply(v -> metadata));
            } else if (entry.isFolder()) {
                try {
                    final RemoteFolder folder = entry.asFolder();
                    final ExternalBlobMetadata metadata = new ExternalBlobMetadata(container, key, folder.folderId(),
                            key != null ? StorageType.FOLDER : StorageType.CONTAINER, blobAccess, BlobHashes.empty(),
                            usermetadata);

                    return this.putWithoutLock(container, key, metadata).thenApply(v -> metadata);
                } catch (IllegalStateException e) {
                    LOGGER.warn("Entry neither folder nor file: {} in container {}", key, container);
                    return CompletableFuture.completedFuture(ExternalBlobMetadata.EMPTY_METADATA);
                }
            } else {
                LOGGER.warn("Entry neither folder nor file: {} in container {}", key, container);
                return CompletableFuture.completedFuture(ExternalBlobMetadata.EMPTY_METADATA);
            }
        } else {
            return CompletableFuture.completedFuture(ExternalBlobMetadata.EMPTY_METADATA);
        }
    }

}
