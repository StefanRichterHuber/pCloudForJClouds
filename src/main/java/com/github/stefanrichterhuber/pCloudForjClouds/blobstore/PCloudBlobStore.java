package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.BaseEncoding.base16;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.MutableStorageMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.Tier;
import org.jclouds.blobstore.domain.internal.MutableStorageMetadataImpl;
import org.jclouds.blobstore.domain.internal.PageSetImpl;
import org.jclouds.blobstore.options.CopyOptions;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.blobstore.reference.BlobStoreConstants;
import org.jclouds.collect.Memoized;
import org.jclouds.domain.Location;
import org.jclouds.http.HttpCommand;
import org.jclouds.http.HttpRequest;
import org.jclouds.http.HttpResponse;
import org.jclouds.http.HttpResponseException;
import org.jclouds.http.HttpUtils;
import org.jclouds.io.ByteStreams2;
import org.jclouds.io.ContentMetadata;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.AbstractBlobStore;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.EmptyPayload;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.HashingBlobDataSource;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudBlobStoreException;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudError;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudMultipartUpload;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudUtils;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.RemoteFilePayload;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudBlobKeyValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudContainerNameValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.common.net.HttpHeaders;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.RemoteEntry;
import com.pcloud.sdk.RemoteFile;
import com.pcloud.sdk.RemoteFolder;
import com.pcloud.sdk.UploadOptions;

/**
 * {@link BlobStore} for pCloud
 * 
 * @see https://docs.pcloud.com/
 * @author Stefan Richter-Huber
 *
 */
@Singleton
public final class PCloudBlobStore extends AbstractBlobStore {

    private static final int MAXIMUM_NUMBER_OF_PARTS = Integer.MAX_VALUE;
    private static final int MAXIMUM_MULTIPART_SIZE = 5 * 1024 * 1024;
    private static final int MINIUM_MULTIPART_SIZE = 1;
    private static final String SEPARATOR = "/";
    private static final byte[] EMPTY_CONTENT = new byte[0];
    @SuppressWarnings("deprecation")
    private static final byte[] DIRECTORY_MD5 = Hashing.md5().hashBytes(EMPTY_CONTENT).asBytes();

    private static final Logger LOGGER = LoggerFactory.getLogger(PCloudBlobStore.class);

    private final Provider<BlobBuilder> blobBuilders;
    private final String baseDirectory;
    private final PCloudContainerNameValidator pCloudContainerNameValidator;
    private final PCloudBlobKeyValidator pCloudBlobKeyValidator;
    private final Supplier<Location> defaultLocation;
    private final ApiClient apiClient;
    private final BlobStoreContext context;
    private final Supplier<Set<? extends Location>> locations;
    private final MultipartUploadFactory multipartUploadFactory;
    private final MetadataStrategy metadataStrategy;
    private final long baseFolderId;
    private final PCloudUtils utils;

    /**
     * Currently active multipart uploads, grouped by the id of the upload
     */
    private final Map<String, PCloudMultipartUpload> currentMultipartUploads = new ConcurrentHashMap<>();

    @Inject
    protected PCloudBlobStore( //
            BlobStoreContext context, Provider<BlobBuilder> blobBuilders, //
            @Named(PCloudConstants.PROPERTY_BASEDIR) String baseDir, //
            ApiClient apiClient, //
            PCloudContainerNameValidator pCloudContainerNameValidator, //
            PCloudBlobKeyValidator pCloudBlobKeyValidator, //
            MetadataStrategy metadataStrategy, //
            @Memoized Supplier<Set<? extends Location>> locations, //
            MultipartUploadFactory multipartUploadFactory, //
            Supplier<Location> defaultLocation //
    ) {
        this.locations = checkNotNull(locations, "locations");
        this.context = checkNotNull(context, "context");
        this.blobBuilders = checkNotNull(blobBuilders, "PCloud storage strategy blobBuilders");
        this.baseDirectory = checkNotNull(baseDir, "Property " + PCloudConstants.PROPERTY_BASEDIR);
        this.pCloudContainerNameValidator = checkNotNull(pCloudContainerNameValidator,
                "PCloud container name validator");
        this.pCloudBlobKeyValidator = checkNotNull(pCloudBlobKeyValidator, "PCloud blob key validator");
        this.defaultLocation = defaultLocation;
        this.apiClient = checkNotNull(apiClient, "PCloud api client");
        this.utils = new PCloudUtils(apiClient);
        this.multipartUploadFactory = checkNotNull(multipartUploadFactory, "multipartupload factory");
        this.baseFolderId = utils.createBaseDirectory(this.baseDirectory).folderId();
        // Directly add the folder to the cache
        this.metadataStrategy = checkNotNull(metadataStrategy, "PCloud metadatastrategy");

    }

    /**
     * Returns local instance of PCloud {@link ApiClient}.
     * 
     * @return {@link ApiClient}
     */
    private ApiClient getApiClient() {
        return this.apiClient;
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
     * List all containers present (= folders in base directory!)
     * 
     * @return
     */
    private Collection<String> getAllContainerNames() {
        try {
            final RemoteFolder rootFolder = this.getApiClient().listFolder(this.baseDirectory).execute();
            final List<String> result = rootFolder.children().stream().filter(re -> re.isFolder()).map(re -> re.name())
                    .collect(Collectors.toList());
            return result;
        } catch (IOException | ApiError e) {
            throw new PCloudBlobStoreException(e);
        }
    }

    /**
     * Asserts the given container exists.
     * 
     * @param container Name of the container
     * 
     */
    private void assertContainerExists(String container) {
        this.pCloudContainerNameValidator.validate(container);
        if (!this.containerExists(container)) {
            LOGGER.warn("Container {} does not exist", container);
            throw new ContainerNotFoundException(container, String.format("Container {} not found in {}", container,
                    this.getAllContainerNames().stream().collect(Collectors.joining(", "))));
        }
    }

    /**
     * Asserts the given blob exists in the given container.
     * 
     * @param container Name of the container
     * @param name      Name of the blob
     */
    private void assertBlobExists(String container, String name) {
        this.pCloudContainerNameValidator.validate(container);
        this.pCloudBlobKeyValidator.validate(name);
        if (!this.blobExists(container, name)) {
            LOGGER.warn("Blob {} does not exist in container {}", name, container);
            throw new KeyNotFoundException(container, name, "Not found");
        }
    }

    /**
     * Checks if the parent folder for the blob exists, if not create it
     * 
     * @param dir Directory to create
     * @return ID of the parent folder for the key
     */
    private CompletableFuture<Long> assureParentFolder(@Nonnull String container, @Nullable String targetKey) {
        targetKey = stripDirectorySuffix(targetKey);
        if (targetKey != null && targetKey.contains(SEPARATOR)) {
            final String key = getFolderOfKey(targetKey);

            final BiFunction<String, String, CompletableFuture<ExternalBlobMetadata>> create = (c, k) -> {
                final String name = getFileOfKey(key);
                return this.assureParentFolder(container, key)
                        .thenCompose(parentFolderId -> PCloudUtils
                                .execute(this.getApiClient().createFolder(parentFolderId, name)))
                        .thenApply(rf -> {
                            ExternalBlobMetadata md = new ExternalBlobMetadata(container, key, rf.folderId(),
                                    StorageType.FOLDER, BlobAccess.PRIVATE, BlobHashes.empty(), Collections.emptyMap());
                            return md;
                        });
            };
            return this.metadataStrategy.getOrCreate(container, key, create).thenApply(em -> em.fileId());
        } else {
            return this.metadataStrategy.get(container, null).thenApply(em -> em.fileId());
        }
    }

    /**
     * Utility method to check if the blob key is actually a directory name
     * 
     * @param key Blob key
     * @return
     */
    private static String getDirectoryBlobSuffix(@Nullable String key) {
        if (key != null) {
            for (String suffix : BlobStoreConstants.DIRECTORY_SUFFIXES) {
                if (key.endsWith(suffix)) {
                    return suffix;
                }
            }
        }
        return null;
    }

    /**
     * Strips the the directory suffix from a directory name
     * 
     * @param key Blob key
     * @return
     */
    public static String stripDirectorySuffix(@Nullable String key) {
        String suffix = getDirectoryBlobSuffix(key);
        if (key != null && suffix != null) {
            return key.substring(0, key.lastIndexOf(suffix));
        }
        return key;
    }

    /**
     * If this key contains a folder structure, get the folders. E.g.: f1/f2/f3/k ->
     * k
     * 
     * @param key Blob key
     * @return pure file name
     */
    public static String getFileOfKey(String key) {

        /*
         * First strip directory suffix
         */
        String name = getDirectoryBlobSuffix(key) != null ? stripDirectorySuffix(key) : key;
        name = name.replace("\\", SEPARATOR);

        /*
         * Then fetch name
         */
        if (name.contains(SEPARATOR)) {
            return name.substring(name.lastIndexOf(SEPARATOR) + 1);
        }
        return name;
    }

    /**
     * If this key contains a folder structure, get the folders. E.g.: f1/f2/f3/k ->
     * f1/f2/f3
     * 
     * @param key Blobname
     * @return Parent folder name
     */
    private static String getFolderOfKey(String key) {
        String folder = getDirectoryBlobSuffix(key) != null ? stripDirectorySuffix(key) : key;
        folder = folder.replace("\\", SEPARATOR);
        if (folder.contains(SEPARATOR)) {
            folder = key.substring(0, key.lastIndexOf(SEPARATOR));
            return folder;
        }
        return key;
    }

    /**
     * Creates a PCloud {@link DataSource} from a {@link Blob}.
     * 
     * @param blob {@link Blob} to get {@link DataSource} from.
     * @return {@link DataSource}.
     * @throws IOException
     */
    private static HashingBlobDataSource dataSourceFromBlob(Blob blob) throws IOException {
        if (blob.getMetadata().getSize() != null && blob.getMetadata().getSize().longValue() != blob.getPayload()
                .getContentMetadata().getContentLength().longValue()) {
            LOGGER.warn("Size ({} bytes) of blob does not equal content length ({} bytes): {}",
                    blob.getMetadata().getSize(), blob.getPayload().getContentMetadata().getContentLength(), blob);
        }
        return new HashingBlobDataSource(blob);
    }

    /**
     * Loads the {@link StorageMetadata} for a container from the
     * {@link #metadataStrategy}.
     * 
     * @param container Container to get metadata for
     * @return {@link StorageMetadata} found.
     */
    private CompletableFuture<StorageMetadata> getContainerMetadata(@Nonnull String container) {
        this.pCloudContainerNameValidator.validate(container);
        return this.metadataStrategy.get(container, null)
                .thenCompose(md -> PCloudUtils.execute(this.getApiClient().loadFolder(md.fileId())))
                .thenApply(remoteFolder -> {
                    MutableStorageMetadata metadata = new MutableStorageMetadataImpl();
                    metadata.setName(container);
                    metadata.setType(StorageType.CONTAINER);
                    metadata.setLocation(defaultLocation.get());
                    metadata.setCreationDate(remoteFolder.created());
                    metadata.setLastModified(remoteFolder.lastModified());
                    metadata.setId(Long.toString(remoteFolder.folderId()));

                    LOGGER.debug("Requested storage metadata for container {}: {}", container, metadata);
                    return (StorageMetadata) metadata;
                })
                .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> (StorageMetadata) null));
    }

    /**
     * Creates a directory blob (simply a new folder within the container)
     * 
     * @param conainerName Name of the container
     * @param blob         Directory blob
     * @return Etag of the blob (contains a default value)
     * @throws IOException
     */
    private String putDirectoryBlob(@Nonnull String containerName, @Nonnull Blob blob, @Nullable PutOptions options)
            throws IOException {
        if (options == null) {
            return putDirectoryBlob(containerName, blob, PutOptions.NONE);
        }
        final String blobName = blob.getMetadata().getName();
        LOGGER.info("Put directory blob {}/{} with options {}", containerName, blobName, options);
        final String directory = stripDirectorySuffix(blobName);

        return assureParentFolder(containerName, blobName)
                .thenCompose(targetFolderId -> {
                    return PCloudUtils.execute(this.getApiClient().createFolder(targetFolderId, directory))
                            .thenCompose(folder -> {
                                // Upload metadata
                                final ExternalBlobMetadata metadata = new ExternalBlobMetadata(containerName, blobName,
                                        folder.folderId(),
                                        StorageType.FOLDER,
                                        options.getBlobAccess(), BlobHashes.empty(),
                                        blob.getMetadata().getUserMetadata());

                                return this.metadataStrategy.put(containerName, blobName, metadata);
                            }) //
                            .thenApply(v -> base16().lowerCase().encode(DIRECTORY_MD5)); //
                })
                .join();
    }

    /**
     * Utility method to quote the given etag if it is not quoted and not null
     * 
     * @param eTag
     * @return
     */
    private static String maybeQuoteETag(String eTag) {
        if (eTag != null && !eTag.startsWith("\"") && !eTag.endsWith("\"")) {
            eTag = "\"" + eTag + "\"";
        }
        return eTag;
    }

    /**
     * Utility method to return a {@link HttpResponseException} with the given HTTP
     * status code.
     * 
     * @param code
     * @return
     */
    private static HttpResponseException returnResponseException(int code) {
        HttpResponse response = HttpResponse.builder().statusCode(code).build();
        return new HttpResponseException(
                new HttpCommand(HttpRequest.builder().method("GET").endpoint("http://stub").build()), response);
    }

    /**
     * This utility method creates a {@link Blob} from a {@link RemoteFile}.
     * 
     * @param remoteFile {@link RemoteFile}.
     * @param metadata   Externalized blob metadata
     * @return {@link Blob} created
     */
    private Blob createBlobFromRemoteEntry(RemoteEntry remoteFile,
            ExternalBlobMetadata metadata) {
        return createBlobFromRemoteEntry(metadata.container(), metadata.key(), remoteFile,
                metadata.hashes() != null ? metadata.hashes().md5() : null,
                metadata.customMetadata());
    }

    /**
     * This utility method creates a {@link Blob} from a {@link RemoteFile}.
     * 
     * @param container  Host container of the file
     * @param remoteFile {@link RemoteFile}.
     * @param metadata   Externalized blob metadata
     * @return {@link Blob} created
     */
    private Blob createBlobFromRemoteEntry(String container, String key, RemoteEntry remoteFile,
            ExternalBlobMetadata metadata) {
        return createBlobFromRemoteEntry(container, key, remoteFile,
                metadata.hashes() != null ? metadata.hashes().md5() : null,
                metadata.customMetadata());
    }

    /**
     * This utility method creates a {@link Blob} from a {@link RemoteFile}.
     * 
     * @param container    Host container of the file
     * @param remoteFile   {@link RemoteFile}.
     * @param md5          MD5 checksum of the file
     * @param userMetadata Custom user metadata for the blob.
     * @return {@link Blob} created
     */
    private Blob createBlobFromRemoteEntry(String container, String key, RemoteEntry remoteFile, String md5,
            Map<String, String> userMetadata) {
        Blob blob = null;

        if (remoteFile instanceof RemoteFile && remoteFile.isFile()) {
            final RemoteFile file = (RemoteFile) remoteFile;
            final Payload payload = file.size() == 0 ? new EmptyPayload() : new RemoteFilePayload(file);
            blob = this.blobBuilders.get() //
                    .payload(payload) //
                    .contentType(file.contentType()) //
                    .contentLength(file.size()) //
                    .contentMD5(md5 != null ? HashCode.fromString(md5) : null) //
                    .eTag(md5) //
                    .name(key) //
                    .userMetadata(userMetadata) //
                    .type(StorageType.BLOB) //
                    .build();
            blob.getMetadata().setSize(file.size());
        } else {
            final Payload payload = new EmptyPayload();
            blob = this.blobBuilders.get() //
                    .payload(payload) //
                    .contentLength(0l) //
                    .eTag(base16().lowerCase().encode(DIRECTORY_MD5)) //
                    .contentMD5(HashCode.fromBytes(DIRECTORY_MD5)) //
                    .name(key.endsWith(SEPARATOR) ? key : key + SEPARATOR) //
                    .userMetadata(userMetadata) //
                    .type(remoteFile.isFolder() ? StorageType.FOLDER : StorageType.BLOB) //
                    .build();
            blob.getMetadata().setSize(0l);
        }
        blob.getMetadata().setLastModified(remoteFile.lastModified());
        blob.getMetadata().setCreationDate(remoteFile.created());
        blob.getMetadata().setId(remoteFile.id());
        blob.getMetadata().setContainer(container);
        blob.getMetadata().setTier(Tier.STANDARD);
        blob.getMetadata().setLocation(this.defaultLocation.get());
        return blob;
    }

    @Override
    public BlobStoreContext getContext() {
        return context;
    }

    @Override
    public BlobBuilder blobBuilder(String name) {
        return this.blobBuilders.get().name(name);
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        return locations.get();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        LOGGER.info("Requested storage metadata for all containers");

        ArrayList<String> containers = new ArrayList<String>(getAllContainerNames());
        Collections.sort(containers);

        return new PageSetImpl<StorageMetadata>(
                FluentIterable.from(containers).transform(new Function<String, StorageMetadata>() {
                    @Override
                    public StorageMetadata apply(String name) {
                        return getContainerMetadata(name).join();
                    }
                }).filter(Predicates.<StorageMetadata>notNull()), null);
    }

    @Override
    public boolean containerExists(String container) {
        this.pCloudContainerNameValidator.validate(container);
        ExternalBlobMetadata containerMetadata = this.metadataStrategy.get(container, null).join();

        boolean result = containerMetadata != null;
        LOGGER.debug("Does container {} exists -> {}", container, result);
        return result;
    }

    @Override
    public boolean createContainerInLocation(Location location, String container) {
        LOGGER.info("Create container {} in location {}", container, location);

        this.pCloudContainerNameValidator.validate(container);
        try {

            final RemoteFolder remoteFolder = this.getApiClient().createFolder(createPath(container)).execute();
            final ExternalBlobMetadata blobMetadata = new ExternalBlobMetadata(container, null, remoteFolder.folderId(),
                    StorageType.CONTAINER, BlobAccess.PRIVATE, BlobHashes.empty(), Collections.emptyMap());
            this.metadataStrategy.put(container, null, blobMetadata);
            return remoteFolder.isFolder();
        } catch (IOException | ApiError e) {
            throw new PCloudBlobStoreException(e);
        }
    }

    @Override
    public boolean createContainerInLocation(Location location, String container, CreateContainerOptions options) {
        return createContainerInLocation(location, container);
    }

    @Override
    public ContainerAccess getContainerAccess(String container) {
        this.pCloudContainerNameValidator.validate(container);
        final ExternalBlobMetadata blobMetadata = this.metadataStrategy.get(container, null).join();
        return blobMetadata != null
                ? (blobMetadata.access() == BlobAccess.PRIVATE ? ContainerAccess.PRIVATE : ContainerAccess.PUBLIC_READ)
                : null;
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess access) {
        this.pCloudContainerNameValidator.validate(container);

        final ExternalBlobMetadata old = this.metadataStrategy.get(container, null).join();

        if (old != null) {
            BlobAccess nxt = null;
            if (old.access() == BlobAccess.PRIVATE && access == ContainerAccess.PUBLIC_READ) {
                nxt = BlobAccess.PUBLIC_READ;
            } else if (old.access() == BlobAccess.PUBLIC_READ && access == ContainerAccess.PRIVATE) {
                nxt = BlobAccess.PRIVATE;
            }
            if (nxt != null) {
                ExternalBlobMetadata nw = new ExternalBlobMetadata(old.container(), old.key(), old.fileId(),
                        old.storageType(), nxt, old.hashes(), old.customMetadata());
                metadataStrategy.put(container, null, nw).join();
            }
            // Nothing to change here
        }
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String containerName, ListContainerOptions options) {
        LOGGER.info("Requested storage metadata for container {} with options {}", containerName, options);

        if (options == null) {
            return list(containerName, ListContainerOptions.NONE);
        }

        // Fetch the metadata
        final PageSet<? extends StorageMetadata> result = this.metadataStrategy
                .list(containerName, options).thenCompose(contents -> {
                    // Fetch the actual blobs
                    final CompletableFuture<PageSet<? extends StorageMetadata>> results = PCloudUtils
                            .allOf(contents.stream().map(md -> {
                                CompletableFuture<? extends StorageMetadata> blobMetadata = PCloudUtils
                                        .execute(this.apiClient.loadFile(md.fileId()))
                                        .thenApply(rf -> this.createBlobFromRemoteEntry(rf, md))
                                        .thenApply(Blob::getMetadata)
                                        .exceptionally(
                                                e -> PCloudUtils.notFileFoundDefault(e,
                                                        () -> {
                                                            LOGGER.warn(
                                                                    "Found metadata for blob that does not exist anymore: {}/{}",
                                                                    md.container(), md.key());
                                                            // Remove old metadata
                                                            this.metadataStrategy.delete(md.container(), md.key())
                                                                    .exceptionally(x -> null)
                                                                    .join();
                                                            return null;
                                                        }));
                                return blobMetadata;
                            }).collect(Collectors.toList()))
                            .thenApply(sm -> sm.stream().filter(e -> e != null)
                                    .collect(Collectors.toCollection(TreeSet::new)))
                            .thenApply(sm -> new PageSetImpl<StorageMetadata>(sm, contents.getNextMarker()));
                    return results;
                }).join();

        return result;

    }

    @Override
    public void deleteContainer(String container) {
        LOGGER.info("Delete container {}", container);

        assertContainerExists(container);

        final String p = createPath(container);
        // Remove folder and all files
        PCloudUtils.execute(this.getApiClient()
                .deleteFolder(p, true))
                .thenCompose(r -> {
                    if (r) {
                        LOGGER.debug("Successfully deleted folder of container {}", container);
                        return this.metadataStrategy.list(container, ListContainerOptions.Builder.recursive())
                                .thenCompose(ps -> {
                                    // Delete metadata of files within the container
                                    return PCloudUtils.allOf(ps.stream()
                                            .map(em -> this.metadataStrategy.delete(em.container(), em.key()))
                                            .collect(Collectors.toList()));
                                })
                                .thenCompose(v -> {
                                    LOGGER.debug("Successfully deleted metadata of files in container {}", container);
                                    // Delete metadata of the container itself
                                    return this.metadataStrategy.delete(container, null).thenApply(m -> true);
                                }).thenApply(v -> {
                                    LOGGER.debug("Successfully deleted metadata of container {}", container);
                                    return v;
                                });
                    } else {
                        LOGGER.warn("Failed to delete folder of container {}", container);
                        return CompletableFuture.completedStage(r);
                    }
                }).join();
    }

    @Override
    public boolean deleteContainerIfEmpty(String container) {
        LOGGER.info("Delete container {} if empty", container);

        assertContainerExists(container);
        try {
            final RemoteFolder folder = this.getApiClient().listFolder(createPath(container)).execute();
            folder.delete(false);
            LOGGER.debug("Successfully deleted empty container {}", container);

            // Invalidate cache
            this.metadataStrategy.delete(container, null).join();

            return true;
        } catch (IOException | ApiError e) {
            LOGGER.warn("Failed to delete empty(?) container {}", container);
            throw new PCloudBlobStoreException(e);
        }
    }

    @Override
    public boolean blobExists(String container, String key) {
        LOGGER.info("Check if blob {}/{} exists", container, key);
        assertContainerExists(container);
        this.pCloudBlobKeyValidator.validate(key);
        return this.metadataStrategy.get(container, key).thenCompose(ebm -> {
            if (ebm == null) {
                // Blob does not exists
                return CompletableFuture.completedFuture(false);
            }
            if (ebm.storageType() == StorageType.FOLDER) {
                return PCloudUtils.execute(this.getApiClient().loadFolder(ebm.fileId())) //
                        .thenApply(f -> f != null) //
                        .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> false));
            } else {
                // Storage type == file
                return PCloudUtils.execute(this.getApiClient().loadFile(ebm.fileId())) //
                        .thenApply(f -> f != null) //
                        .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> false));
            }
        }).join();
    }

    @Override
    public String putBlob(String container, Blob blob, PutOptions options) {
        assertContainerExists(container);
        final String key = blob.getMetadata().getName();
        this.pCloudBlobKeyValidator.validate(key);

        LOGGER.info("Put blob {}/{} with options {}", container, key, options);
        try {
            if (getDirectoryBlobSuffix(key) != null || blob.getMetadata().getType() == StorageType.FOLDER) {
                return putDirectoryBlob(container, blob, options);
            }

            final String name = getFileOfKey(key);

            // Check if parent folder exists
            final long targetFolderId = assureParentFolder(container, key).join();

            // First write file (and generate hash codes)
            final HashingBlobDataSource ds = dataSourceFromBlob(blob);
            final RemoteFile uploadedFile = PCloudUtils.execute(this.getApiClient().createFile(
                    targetFolderId, name, ds, blob.getMetadata().getLastModified(), null, UploadOptions.OVERRIDE_FILE))
                    .join();

            // Check if md5 hashes match

            if (blob.getMetadata().getContentMetadata().getContentMD5AsHashCode() != null) {
                final String sendMD5 = blob.getMetadata().getContentMetadata().getContentMD5AsHashCode().toString();
                if (sendMD5 != null && !sendMD5.equals(ds.getHashes().md5())) {
                    LOGGER.warn("Blob {}/{}: Client delivered MD5 {} does not match calculated MD5 during transfer {}",
                            container, key, sendMD5, ds.getHashes().md5());
                }
            }

            // Write the file metadata
            final ExternalBlobMetadata md = new ExternalBlobMetadata(container, key,
                    uploadedFile.fileId(),
                    StorageType.BLOB,
                    options.getBlobAccess(),
                    ds.getHashes().withBuildin(uploadedFile.hash()),
                    blob.getMetadata().getUserMetadata());

            this.metadataStrategy.put(container, key, md).join();

            return ds.getHashes().md5();
        } catch (IOException e) {
            throw new PCloudBlobStoreException(e);
        }
    }

    @Override
    public String copyBlob(String fromContainer, String fromName, String toContainer, String toName,
            CopyOptions options) {
        assertContainerExists(fromContainer);
        assertBlobExists(fromContainer, fromName);
        if (!fromContainer.equals(toContainer)) {
            assertContainerExists(toContainer);
        }

        try {
            GetOptions getOptions = null;
            if (options != null && options != CopyOptions.NONE) {
                // Check the metadata of the source
                getOptions = new GetOptions();
                if (options.ifMatch() != null)
                    getOptions.ifETagMatches(options.ifMatch());
                if (options.ifNoneMatch() != null)
                    getOptions.ifETagDoesntMatch(options.ifNoneMatch());
                if (options.ifModifiedSince() != null)
                    getOptions.ifModifiedSince(options.ifModifiedSince());
                if (options.ifUnmodifiedSince() != null)
                    getOptions.ifUnmodifiedSince(options.ifUnmodifiedSince());

            }
            // Exceptions are thrown if etag / modifications dates do not pass
            Blob source = this.getBlob(fromContainer, fromName, getOptions);

            if (source != null && source.getPayload().getRawContent() instanceof RemoteFile) {

                final RemoteFile sourceFile = (RemoteFile) source.getPayload().getRawContent();
                final long folderId = this.assureParentFolder(toContainer, toName).join();
                RemoteEntry result = null;
                if (fromName.equals(toName)) {
                    result = this.getApiClient().copy(sourceFile.id(), folderId).execute();
                } else {
                    /*
                     * Since the pCloud java api does not support rename on copy, copy source file
                     * to a temporary folder, rename it there and then move it to the parent folder
                     */
                    final String temporaryFolderName = ".tmp-" + UUID.randomUUID().toString();
                    final RemoteFolder temp = this.getApiClient()
                            .createFolder(folderId, temporaryFolderName).execute();
                    final RemoteEntry tempFile = this.getApiClient().copy(sourceFile, temp).execute();
                    final RemoteEntry renamedFile = tempFile.rename(toName);
                    result = this.getApiClient().move(renamedFile.id(), folderId).execute();
                    // Clean up and delete temporary folder
                    temp.delete();

                    // Copy metadata
                    if (result.isFile()) {
                        final ExternalBlobMetadata srcMetadata = this.metadataStrategy.get(fromContainer, fromName)
                                .join();
                        if (srcMetadata != null) {
                            ExternalBlobMetadata trgtMetadata = new ExternalBlobMetadata(toContainer, toName,
                                    result.asFile().fileId(), StorageType.BLOB, srcMetadata.access(),
                                    srcMetadata.hashes().withBuildin(result.asFile().hash()),
                                    srcMetadata.customMetadata());
                            this.metadataStrategy.put(toContainer, toName, trgtMetadata).join();
                            return trgtMetadata.hashes().md5();
                        }
                    }
                    return null;
                }

                return null;
            } else {
                // The source is no real file
                throw new KeyNotFoundException(fromContainer, fromName,
                        "Source not found / is not a real file during copy");
            }

        } catch (ApiError e) {
            if (PCloudError.isEntryNotFound(e)) {
                throw new KeyNotFoundException(fromContainer, fromName,
                        "Source not found / is not a real file during copy");
            }
            throw new PCloudBlobStoreException(e);
        } catch (IOException e) {
            throw new PCloudBlobStoreException(e);
        }
    }

    /**
     * Returns a {@link Blob} of type folder
     * 
     * @param container Container containing the blob
     * @param name      Name of the blob
     * @param metadata  Metadata of the blob
     * @param options   {@link GetOptions} to apply
     * @return {@link Blob} found or null, if blob does not exist.
     */
    private Blob getDirectoryBlob(String container, String name, ExternalBlobMetadata metadata, GetOptions options) {
        LOGGER.info("Get directory blob {}/{} with options {}", container, name, options);

        final RemoteFolder remoteFile = PCloudUtils
                .execute(this.getApiClient().loadFolder(metadata.fileId())).join();
        final Blob blob = createBlobFromRemoteEntry(container, name, remoteFile, metadata);
        return blob;
    }

    @Override
    public Blob getBlob(String container, String name, GetOptions options) {
        assertContainerExists(container);
        try {
            LOGGER.info("Get blob {}/{} with options {}", container, name, options);
            final ExternalBlobMetadata metadata = this.metadataStrategy.get(container, name).join();
            if (metadata == null) {
                // No metadata - no file
                return null;
            }
            if (metadata.storageType() == StorageType.FOLDER) {
                return this.getDirectoryBlob(container, name, metadata, options);
            }

            final RemoteFile remoteFile = PCloudUtils
                    .execute(this.getApiClient().loadFile(metadata.fileId())).join();
            final Blob blob = createBlobFromRemoteEntry(container, name, remoteFile, metadata);

            if (options != null && options != GetOptions.NONE) {
                final String eTag = maybeQuoteETag(metadata.hashes().md5());
                if (eTag != null) {
                    if (options.getIfMatch() != null) {
                        if (!eTag.equals(maybeQuoteETag(options.getIfMatch())))
                            throw returnResponseException(412);
                    }
                    if (options.getIfNoneMatch() != null) {
                        if (eTag.equals(maybeQuoteETag(options.getIfNoneMatch())))
                            throw returnResponseException(304);
                    }
                }

                if (options.getIfModifiedSince() != null) {
                    Date modifiedSince = options.getIfModifiedSince();
                    if (remoteFile.lastModified().before(modifiedSince)) {
                        HttpResponse response = HttpResponse.builder().statusCode(304).build();
                        throw new HttpResponseException(
                                String.format("%1$s is before %2$s", remoteFile.lastModified(), modifiedSince), null,
                                response);
                    }

                }
                if (options.getIfUnmodifiedSince() != null) {
                    Date unmodifiedSince = options.getIfUnmodifiedSince();
                    if (remoteFile.lastModified().after(unmodifiedSince)) {
                        HttpResponse response = HttpResponse.builder().statusCode(412).build();
                        throw new HttpResponseException(
                                String.format("%1$s is after %2$s", remoteFile.lastModified(), unmodifiedSince), null,
                                response);
                    }
                }

                if (options.getRanges() != null && !options.getRanges().isEmpty()) {
                    long size = 0;
                    final ImmutableList.Builder<ByteSource> streams = ImmutableList.builder();

                    final ByteSource byteSource = ByteSource
                            .wrap(ByteStreams2.toByteArrayAndClose(remoteFile.byteStream()));

                    for (String s : options.getRanges()) {
                        // HTTP uses a closed interval while Java array indexing uses a
                        // half-open interval.
                        long offset = 0;
                        long last = remoteFile.size() - 1;
                        if (s.startsWith("-")) {
                            offset = last - Long.parseLong(s.substring(1)) + 1;
                            if (offset < 0) {
                                offset = 0;
                            }
                        } else if (s.endsWith("-")) {
                            offset = Long.parseLong(s.substring(0, s.length() - 1));
                        } else if (s.contains("-")) {
                            String[] firstLast = s.split("\\-");
                            offset = Long.parseLong(firstLast[0]);
                            last = Long.parseLong(firstLast[1]);
                        } else {
                            throw new HttpResponseException("illegal range: " + s, null,
                                    HttpResponse.builder().statusCode(416).build());
                        }

                        if (offset >= remoteFile.size()) {
                            throw new HttpResponseException("illegal range: " + s, null,
                                    HttpResponse.builder().statusCode(416).build());
                        }
                        if (last + 1 > remoteFile.size()) {
                            last = remoteFile.size() - 1;
                        }
                        streams.add(byteSource.slice(offset, last - offset + 1));
                        size += last - offset + 1;
                        blob.getAllHeaders().put(HttpHeaders.CONTENT_RANGE, "bytes " + offset + "-" + last + "/"
                                + blob.getPayload().getContentMetadata().getContentLength());
                    }
                    ContentMetadata cmd = blob.getPayload().getContentMetadata();
                    blob.setPayload(ByteSource.concat(streams.build()).openStream());
                    HttpUtils.copy(cmd, blob.getPayload().getContentMetadata());
                    blob.getPayload().getContentMetadata().setContentLength(size);
                    blob.getMetadata().setSize(size);
                }
            }
            return blob;
        } catch (Exception e) {
            return PCloudUtils.notFileFoundDefault(e, () -> null, PCloudBlobStoreException::new);
        }
    }

    /**
     * Utility method to remove a blob with its externalized metadata
     * 
     * @param container Container containing the blob
     * @param name      Name of the blob
     * @return Success of the operation
     */
    private CompletableFuture<Boolean> removeBlobWithMetadata(String container, String name) {
        final CompletableFuture<Boolean> deleteJob = this.metadataStrategy.get(container, name)
                .thenCompose(md -> {
                    if (md == null) {
                        // File / Folder does not exists
                        return CompletableFuture.completedFuture(false);
                    }
                    if (md.storageType() == StorageType.FOLDER) {
                        // This is a folder. Remove the folder, the cached data of the folder and
                        // folders within the folder and metadata of the folder and blobs within the
                        // folder
                        return PCloudUtils.execute(this.getApiClient().deleteFolder(md.fileId(), true)) //
                                .thenCompose(
                                        r -> r ? this.metadataStrategy.delete(container, name).thenApply(v -> r)
                                                : CompletableFuture.completedFuture(r))
                                .thenCompose(r -> {
                                    if (r) {
                                        // Remote metadata of files inside the folder
                                        return this.metadataStrategy
                                                .list(container,
                                                        ListContainerOptions.Builder.recursive().prefix(name))
                                                .thenCompose(ps -> PCloudUtils
                                                        .allOf(ps.stream()
                                                                .map(em -> this.metadataStrategy.delete(em.container(),
                                                                        em.key()))
                                                                .collect(Collectors.toList())))
                                                .thenApply(v -> r);
                                    } else {
                                        return CompletableFuture.completedFuture(r);
                                    }
                                });
                    }
                    if (md.storageType() == StorageType.CONTAINER) {
                        return CompletableFuture.supplyAsync(() -> {
                            this.deleteContainer(name);
                            return true;
                        });
                    } else {
                        // This is a file. Simply remove the file and its metadata
                        return PCloudUtils.execute(this.getApiClient().deleteFile(md.fileId()))
                                .thenCompose(
                                        r -> r ? this.metadataStrategy.delete(container, name).thenApply(v -> r)
                                                : CompletableFuture.completedFuture(r));
                    }
                });
        return deleteJob;
    }

    @Override
    public void removeBlob(String container, String name) {
        LOGGER.info("Remove blob {}/{}", container, name);

        this.pCloudContainerNameValidator.validate(container);
        boolean result = this.removeBlobWithMetadata(container, name).join();
        if (!result) {
            LOGGER.warn("Failed to delete {}{}{}", container, SEPARATOR, name);
        }
    }

    @Override
    public void removeBlobs(String container, Iterable<String> names) {
        LOGGER.info("Remove blobs {} in container {}", names, container);

        assertContainerExists(container);
        if (names != null) {
            // Start all remove jobs in parallel
            final Map<String, CompletableFuture<Boolean>> jobs = new HashMap<>();
            for (String name : names) {
                jobs.put(name, this.removeBlobWithMetadata(container, name));
            }
            for (Entry<String, CompletableFuture<Boolean>> entry : jobs.entrySet()) {
                if (entry.getValue().join()) {
                    LOGGER.debug("Successfully deleted {}{}{}", container, entry.getKey());
                } else {
                    LOGGER.error("Failed to delete {}{}{}", container, SEPARATOR, entry.getKey());
                }
            }
        }
    }

    @Override
    public BlobAccess getBlobAccess(String container, String name) {

        assertContainerExists(container);
        final Optional<ExternalBlobMetadata> readMetadata = Optional
                .fromNullable(this.metadataStrategy.get(container, name).join());
        BlobAccess blobAccess = readMetadata.transform(ExternalBlobMetadata::access).or(BlobAccess.PRIVATE);
        LOGGER.info("Get blob access {}/{} ->  {}", container, name, blobAccess);
        return blobAccess;
    }

    @Override
    public void setBlobAccess(String container, String name, BlobAccess access) {
        LOGGER.info("Set blob access {}/{} to {}", container, name, access);

        assertContainerExists(container);
        assertBlobExists(container, name);

        final Optional<ExternalBlobMetadata> readMetadata = Optional
                .fromNullable(this.metadataStrategy.get(container, name).join());
        if (readMetadata.isPresent() && readMetadata.get().access() != access) {
            final ExternalBlobMetadata oldMd = readMetadata.get();
            final ExternalBlobMetadata newMd = new ExternalBlobMetadata(oldMd.container(), oldMd.key(), oldMd.fileId(),
                    oldMd.storageType(),
                    access, oldMd.hashes(), oldMd.customMetadata());
            this.metadataStrategy.put(container, name, newMd).join();
        }
    }

    /**
     * Utility method to find the {@link PCloudMultipartUpload} implementation for a
     * given {@link MultipartUpload} instance
     * 
     * @param mpu
     * @return
     */
    private PCloudMultipartUpload resolveUpload(MultipartUpload mpu) {
        if (mpu instanceof PCloudMultipartUpload) {
            return (PCloudMultipartUpload) mpu;
        } else {
            final PCloudMultipartUpload pCloudMultipartUpload = this.currentMultipartUploads.get(mpu.id());
            if (pCloudMultipartUpload != null) {
                return pCloudMultipartUpload;
            } else {
                throw new IllegalStateException("No on-going upload found for id " + mpu.id());
            }
        }
    }

    @Override
    public MultipartUpload initiateMultipartUpload(String container, BlobMetadata blobMetadata, PutOptions options) {

        assertContainerExists(container);
        final String uploadId = UUID.randomUUID().toString();

        LOGGER.info("Initiate multipart upload to container {} with data {} and options {} -> upload id {}", container,
                blobMetadata,
                options, uploadId);

        final String name = getFileOfKey(blobMetadata.getName());

        // Check if parent folder exists
        final long folderId = assureParentFolder(container, blobMetadata.getName()).join();
        PCloudMultipartUpload upload = this.multipartUploadFactory.create(folderId, container, name,
                uploadId, blobMetadata, options);
        upload.start();
        this.currentMultipartUploads.put(uploadId, upload);
        return upload;
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        LOGGER.info("Abort multipart upload with id {}", mpu.id());
        resolveUpload(mpu).abort();
        this.currentMultipartUploads.remove(mpu.id());
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
        LOGGER.info("Complete multipart upload with id {} and parts {}", mpu.id(), parts);
        final String etag = resolveUpload(mpu).complete().join();
        this.currentMultipartUploads.remove(mpu.id());
        return etag;
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {
        LOGGER.info("Upload part {} to multipart upload with id {}", partNumber, mpu.id());
        try {
            return resolveUpload(mpu).append(partNumber, payload).join();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
        List<MultipartPart> parts = resolveUpload(mpu).getParts();
        LOGGER.info("List parts of multipart upload with id {} -> {}", mpu.id(), parts);
        return parts;
    }

    @Override
    public List<MultipartUpload> listMultipartUploads(String container) {

        final List<MultipartUpload> result = new ArrayList<>(this.currentMultipartUploads.values().stream()
                .filter(v -> v.containerName().equals(container)).collect(Collectors.toList()));
        if (result.size() > 0) {
            LOGGER.debug("Found active multipart uploads for container {}: {}", container, result);
        }
        LOGGER.info("List multipart uploads for container {} -> {}", container, result);
        return result;
    }

    @Override
    public long getMinimumMultipartPartSize() {
        LOGGER.info("Get minium multipart size -> {} byte(s)", MINIUM_MULTIPART_SIZE);
        return MINIUM_MULTIPART_SIZE;
    }

    @Override
    public long getMaximumMultipartPartSize() {
        LOGGER.info("Get minium multipart size -> {} bytes", MAXIMUM_MULTIPART_SIZE);
        return MAXIMUM_MULTIPART_SIZE;
    }

    @Override
    public int getMaximumNumberOfParts() {
        LOGGER.info("Get maximum number of parts -> {}", MAXIMUM_NUMBER_OF_PARTS);
        return MAXIMUM_NUMBER_OF_PARTS;
    }

}
