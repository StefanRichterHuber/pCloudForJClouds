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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.checkerframework.checker.nullness.qual.Nullable;
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
    /**
     * Currently active multipart uploads, grouped by the id of the upload
     */
    private final Map<String, PCloudMultipartUpload> currentMultipartUploads = new ConcurrentHashMap<>();
    /**
     * It is necessary to ensure the existence of the parent folder before
     * {@link #putBlob(String, Blob, PutOptions)} using
     * {@link #assureParentFolder(String)}. Since it is inefficient to fetch folders
     * by path, cache the folder id for each folder accessed.
     */
    private final Map<String, RemoteFolder> remoteFolderCache = new ConcurrentHashMap<String, RemoteFolder>();

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
        this.multipartUploadFactory = checkNotNull(multipartUploadFactory, "multipartupload factory");
        RemoteFolder baseFolder = PCloudUtils.createBaseDirectory(this.apiClient, this.baseDirectory);
        // Directly add the folder to the cache
        this.remoteFolderCache.put(baseDirectory, baseFolder);
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
     * Checks if the directory exists, and creates it if not.
     * 
     * @param dir Directory to create
     * @return
     */
    private RemoteFolder assureParentFolder(String dir) throws IOException, ApiError {

        final RemoteFolder remoteFolder = this.remoteFolderCache.compute(dir, this::checkOrFetchFolder);
        if (remoteFolder != null) {
            return remoteFolder;
        }

        // Remote folder does not exists, check for its parent
        final String parentDir = dir.substring(0, dir.lastIndexOf(SEPARATOR));
        final RemoteFolder parentFolder = assureParentFolder(parentDir);

        // Now check again and create folder if still necessary
        final RemoteFolder result = this.remoteFolderCache.computeIfAbsent(dir,
                k -> this.createOrFetchFolder(parentFolder.folderId(), k));
        return result;
    }

    /**
     * Tries to create the given folder. If it fails because the backend already
     * knows the folder, fetch it.
     * 
     * @param dir
     * @param parentFolderId Id of the parent folder
     * @return
     */
    private RemoteFolder createOrFetchFolder(long parentFolderId, String dir) {
        final String name = dir.substring(dir.lastIndexOf(SEPARATOR) + 1);
        try {
            final RemoteFolder rf = this.apiClient.createFolder(parentFolderId, name).execute();
            if (rf != null) {
                LOGGER.info("Parent folder {} with id {} created!", dir, rf.folderId());
            }
            return rf;
        } catch (ApiError e) {
            final PCloudError pCloudError = PCloudError.parse(e);
            if (pCloudError == PCloudError.ALREADY_EXISTS) {
                // File already exists, try to fetch it from the parent folder
                try {
                    RemoteFolder remoteFolder = this.apiClient.loadFolder(parentFolderId).execute().children().stream()
                            .filter(RemoteEntry::isFolder).map(RemoteEntry::asFolder)
                            .filter(rf -> rf.name().equals(name)).findFirst().orElse(null);
                    return remoteFolder;
                } catch (IOException | ApiError e1) {
                    // Ignore --> just throw the previous exception
                }
            }
            throw new PCloudBlobStoreException(e);
        } catch (IOException e) {
            throw new PCloudBlobStoreException(e);
        }
    }

    /**
     * Checks if the given folder still exists, of if null, check if a folder for
     * the given dir exists.
     * 
     * @param dir
     * @param v
     * @return
     */
    private RemoteFolder checkOrFetchFolder(String dir, RemoteFolder v) {
        if (v != null) {
            // Remotefolder in cache, check if still present
            try {
                RemoteFolder remoteFolder = this.apiClient.loadFolder(v.folderId()).execute();
                if (remoteFolder != null) {
                    LOGGER.debug("Parent folder {} with id {} exists in cache and remote.", dir, v.folderId());
                    return remoteFolder;
                }
            } catch (IOException | ApiError e) {
                LOGGER.debug("Parent folder {} with id {} exists in cache but not remote!", dir, v.folderId());
                // Folder does not exist anymore
                return null;
            }
        }
        // Folder not in cache, check if present remote
        try {
            RemoteFolder remoteFolder = this.apiClient.listFolder(this.createPath(dir)).execute();
            LOGGER.debug("Parent folder {} exists only remote, but not yet in cache.", dir);
            return remoteFolder;
        } catch (IOException | ApiError e) {
            LOGGER.debug("Parent folder {} does not exist.", dir);
            // Folder does not exist remote
            return null;
        }
    }

    /**
     * Utility method to check if the blob key is actually a directory name
     * 
     * @param key Blob key
     * @return
     */
    private static String getDirectoryBlobSuffix(String key) {
        for (String suffix : BlobStoreConstants.DIRECTORY_SUFFIXES) {
            if (key.endsWith(suffix)) {
                return suffix;
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
    public static String stripDirectorySuffix(String key) {
        String suffix = getDirectoryBlobSuffix(key);
        if (suffix != null) {
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
    private static String getFileOfKey(String key) {

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
    private static String getFolderOfKey(String container, String key) {
        String folder = getDirectoryBlobSuffix(key) != null ? stripDirectorySuffix(key) : key;
        folder = folder.replace("\\", SEPARATOR);
        if (folder.contains(SEPARATOR)) {
            folder = key.substring(0, key.lastIndexOf(SEPARATOR));
            return container + SEPARATOR + folder;
        }
        return container;
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

    private StorageMetadata getContainerMetadata(String container) {
        this.pCloudContainerNameValidator.validate(container);

        try {
            RemoteFolder remoteFolder = this.getApiClient().loadFolder(this.createPath(container)).execute();

            MutableStorageMetadata metadata = new MutableStorageMetadataImpl();
            metadata.setName(container);
            metadata.setType(StorageType.CONTAINER);
            metadata.setLocation(defaultLocation.get());
            metadata.setCreationDate(remoteFolder.created());
            metadata.setLastModified(remoteFolder.lastModified());
            metadata.setId(remoteFolder.id());

            LOGGER.debug("Requested storage metadata for container {}: {}", container, metadata);

            return metadata;
        } catch (Exception e) {
            return PCloudUtils.notFileFoundDefault(e, () -> null, PCloudBlobStoreException::new);
        }
    }

    /**
     * Creates a directory blob (simply a new folder within the container)
     * 
     * @param conainerName Name of the container
     * @param blob         Directory blob
     * @return Etag of the blob (contains a default value)
     * @throws IOException
     */
    private String putDirectoryBlob(String containerName, Blob blob) throws IOException {
        final String blobName = blob.getMetadata().getName();
        final String directory = stripDirectorySuffix(blobName);
        try {
            this.getApiClient().createFolder(this.createPath(containerName, directory)).execute();
            return base16().lowerCase().encode(DIRECTORY_MD5);
        } catch (IOException | ApiError e) {
            throw new PCloudBlobStoreException(e);
        }
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
            // Add entry to cache
            if (remoteFile instanceof RemoteFolder) {
                this.remoteFolderCache.put(key, (RemoteFolder) remoteFile);
            }

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
                        return getContainerMetadata(name);
                    }
                }).filter(Predicates.<StorageMetadata>notNull()), null);
    }

    @Override
    public boolean containerExists(String container) {

        this.pCloudContainerNameValidator.validate(container);
        RemoteFolder remoteFolder = this.remoteFolderCache.compute(container, this::checkOrFetchFolder);
        LOGGER.info("Does container {} exists -> {}", container, remoteFolder != null);
        return remoteFolder != null;
    }

    @Override
    public boolean createContainerInLocation(Location location, String container) {
        LOGGER.info("Create container {} in location {}", container, location);

        this.pCloudContainerNameValidator.validate(container);
        try {
            RemoteFolder remoteFolder = this.getApiClient().createFolder(createPath(container)).execute();
            this.remoteFolderCache.put(container, remoteFolder);
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
        LOGGER.warn("Retrieving access rights for PCloud containers not supported: {}", container);
        return ContainerAccess.PRIVATE;
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess access) {
        this.pCloudContainerNameValidator.validate(container);
        LOGGER.warn("Settings access rights for PCloud containers not supported: {}", container);

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
                                CompletableFuture<StorageMetadata> blobMetadata = PCloudUtils
                                        .execute(this.apiClient.loadFile(md.fileId()))
                                        .thenApply(rf -> this.createBlobFromRemoteEntry(rf, md))
                                        .thenApply(Blob::getMetadata);
                                return blobMetadata;
                            }).collect(Collectors.toList()), TreeSet::new)
                            .thenApply(sm -> new PageSetImpl<StorageMetadata>(sm, contents.getNextMarker()));
                    return results;
                }).join();

        return result;
    }

    @Override
    public void deleteContainer(String container) {
        LOGGER.info("Delete container {}", container);

        assertContainerExists(container);
        try {
            // Remove folder and all files
            final RemoteFolder folder = this.getApiClient().listFolder(createPath(container)).execute();
            folder.delete(true);

            // invalidate cache
            this.remoteFolderCache.remove(container);
            // Check for and remove child entries
            final String childEntryPrefix = container + SEPARATOR;
            final Iterator<Entry<String, RemoteFolder>> it = this.remoteFolderCache.entrySet().iterator();
            while (it.hasNext()) {
                final Entry<String, RemoteFolder> entry = it.next();
                if (entry.getKey().startsWith(childEntryPrefix)) {
                    it.remove();
                }
            }

            // Delete metadata
            this.metadataStrategy.list(container, ListContainerOptions.Builder.recursive())
                    .thenCompose(ps -> PCloudUtils
                            .allOf(ps.stream().map(em -> this.metadataStrategy.delete(em.container(), em.key()))
                                    .collect(Collectors.toList())))
                    .join();

            LOGGER.debug("Successfully deleted container {}", container);
        } catch (IOException | ApiError e) {
            throw new PCloudBlobStoreException(e);
        }

    }

    @Override
    public boolean deleteContainerIfEmpty(String container) {
        LOGGER.info("Delete container {} if empty", container);

        assertContainerExists(container);
        try {
            final RemoteFolder folder = this.getApiClient().listFolder(createPath(container)).execute();
            folder.delete(false);
            LOGGER.debug("Successfully deleted empty container {}", container);
            // Since the folder is empty, there is no need to invalidate child cache
            // entries!
            this.remoteFolderCache.remove(container);

            // SInce the folder is empty, there is no need delete metadata
            return true;
        } catch (IOException | ApiError e) {
            LOGGER.warn("Failed to delete empty(?) container {}", container);
            throw new PCloudBlobStoreException(e);
        }
    }

    @Override
    public boolean directoryExists(String container, String directory) {
        LOGGER.info("Check if directory {} exists in container {}", directory, container);

        assertContainerExists(container);
        directory = stripDirectorySuffix(directory);
        RemoteFolder remoteFolder = this.remoteFolderCache.compute(container + SEPARATOR + directory,
                this::checkOrFetchFolder);
        return remoteFolder != null;
    }

    @Override
    public void createDirectory(String container, String directory) {
        LOGGER.info("Create directory {} in container {}", directory, container);

        assertContainerExists(container);
        this.pCloudBlobKeyValidator.validate(directory);
        directory = stripDirectorySuffix(directory);
        try {
            RemoteFolder remoteFolder = this.getApiClient().createFolder(this.createPath(container, directory))
                    .execute();
            this.remoteFolderCache.put(container + SEPARATOR + directory, remoteFolder);
        } catch (IOException | ApiError e) {
            throw new PCloudBlobStoreException(e);
        }

    }

    @Override
    public void deleteDirectory(String containerName, String directory) {
        LOGGER.info("Delete directory {} in container {}", directory, containerName);

        assertContainerExists(containerName);
        directory = stripDirectorySuffix(directory);
        try {
            this.getApiClient().deleteFolder(createPath(containerName, directory)).execute();

            // invalidate cache
            this.remoteFolderCache.remove(containerName + SEPARATOR + directory);
            // Check for and remove child entries
            final String childEntryPrefix = containerName + SEPARATOR + directory + SEPARATOR;
            final Iterator<Entry<String, RemoteFolder>> it = this.remoteFolderCache.entrySet().iterator();
            while (it.hasNext()) {
                final Entry<String, RemoteFolder> entry = it.next();
                if (entry.getKey().startsWith(childEntryPrefix)) {
                    it.remove();
                }
            }

            // Remote metadata
            this.metadataStrategy.list(containerName, ListContainerOptions.Builder.recursive().prefix(directory))
                    .thenCompose(ps -> PCloudUtils
                            .allOf(ps.stream().map(em -> this.metadataStrategy.delete(em.container(), em.key()))
                                    .collect(Collectors.toList())))
                    .join();
        } catch (IOException | ApiError e) {
            throw new PCloudBlobStoreException(e);
        }

    }

    @Override
    public boolean blobExists(String container, String key) {
        LOGGER.info("Check if blob {}/{} exists", container, key);
        assertContainerExists(container);
        this.pCloudBlobKeyValidator.validate(key);

        return this.metadataStrategy.get(container, key).thenCompose(ebm -> {
            if (ebm != null) {
                return PCloudUtils.execute(this.apiClient.loadFile(ebm.fileId())) //
                        .thenApply(rf -> true) //
                        .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> false,
                                PCloudBlobStoreException::new)) //
                        .thenCompose(r -> {
                            // If file was not found, but metadata was, remove metadata
                            return r ? CompletableFuture.completedFuture(r)
                                    : this.metadataStrategy.delete(container, key).thenApply(v -> r);
                        });
            }
            return CompletableFuture.completedFuture(false);
        }).join();
    }

    @Override
    public String putBlob(String container, Blob blob, PutOptions options) {
        assertContainerExists(container);
        this.pCloudBlobKeyValidator.validate(blob.getMetadata().getName());

        LOGGER.info("Put blob {}/{} with options {}", container, blob, options);
        try {
            if (getDirectoryBlobSuffix(blob.getMetadata().getName()) != null) {
                return putDirectoryBlob(container, blob);
            }

            final String name = getFileOfKey(blob.getMetadata().getName());

            final String rootDir = getFolderOfKey(container, blob.getMetadata().getName());

            // Check if parent folder exists
            final RemoteFolder targetFolder = assureParentFolder(rootDir);

            final HashingBlobDataSource ds = dataSourceFromBlob(blob);
            final CompletableFuture<RemoteFile> fileRequest = PCloudUtils.execute(this.getApiClient().createFile(
                    targetFolder, name, ds, blob.getMetadata().getLastModified(), null, UploadOptions.OVERRIDE_FILE));

            // First write file (and generate hash codes)
            final RemoteFile uploadedFiled = fileRequest.join();

            // Then write metadata
            final ExternalBlobMetadata md = new ExternalBlobMetadata(container, blob.getMetadata().getName(),
                    uploadedFiled.fileId(),
                    BlobAccess.PRIVATE,
                    ds.getHashes().withBuildin(uploadedFiled.hash()),
                    blob.getMetadata().getUserMetadata());

            // Write the file metadata
            this.metadataStrategy.put(container, blob.getMetadata().getName(), md).join();

            return ds.getHashes().md5();
        } catch (IOException | ApiError e) {
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
                final RemoteFolder targetFolder = this.assureParentFolder(getFolderOfKey(toContainer, toName));
                RemoteEntry result = null;
                if (fromName.equals(toName)) {
                    result = this.getApiClient().copy(sourceFile, targetFolder).execute();
                } else {
                    /*
                     * Since the pCloud java api does not support rename on copy, copy source file
                     * to a temporary folder, rename it there and then move it to the parent folder
                     */
                    final String temporaryFolderName = ".tmp-" + UUID.randomUUID().toString();
                    final RemoteFolder temp = this.getApiClient()
                            .createFolder(targetFolder.folderId(), temporaryFolderName).execute();
                    final RemoteEntry tempFile = this.getApiClient().copy(sourceFile, temp).execute();
                    final RemoteEntry renamedFile = tempFile.rename(toName);
                    result = renamedFile.move(targetFolder);
                    // Clean up and delete temporary folder
                    temp.delete();

                    // Copy metadata
                    if (result.isFile()) {
                        @Nullable
                        final ExternalBlobMetadata srcMetadata = Optional
                                .fromNullable(this.metadataStrategy.get(fromContainer, fromName).join()).orNull();
                        if (srcMetadata != null) {
                            ExternalBlobMetadata trgtMetadata = new ExternalBlobMetadata(toContainer, toName,
                                    result.asFile().fileId(), srcMetadata.access(),
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

    @Override
    public Blob getBlob(String container, String name, GetOptions options) {
        assertContainerExists(container);
        try {
            LOGGER.info("Get blob {}/{} with options {}", container, name, options);
            // Remove final directory separator when fetching a directory
            name = stripDirectorySuffix(name);

            @Nullable
            ExternalBlobMetadata metadata = Optional.fromNullable(this.metadataStrategy.get(container, name).join())
                    .orNull();
            if (metadata == null) {
                // No metadata - no file
                return null;
            }
            final RemoteFile remoteFile = PCloudUtils
                    .execute(this.getApiClient().loadFile(metadata.fileId())).get();
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
        final CompletableFuture<Boolean> deleteJob = CompletableFuture.supplyAsync(() -> {
            @Nullable
            ExternalBlobMetadata metadata = Optional.fromNullable(this.metadataStrategy.get(container, name).join())
                    .orNull();
            return metadata;
        }).thenCompose(md -> {
            return md == null ? CompletableFuture.completedFuture(false)
                    : PCloudUtils.execute(this.getApiClient().deleteFile(md.fileId()))
                            .thenCompose(r -> r ? this.metadataStrategy.delete(container, name).thenApply(v -> r)
                                    : CompletableFuture.completedFuture(r));
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
                try {
                    if (entry.getValue().get()) {
                        LOGGER.debug("Successfully deleted {}{}{}", container, entry.getKey());
                    } else {
                        LOGGER.error("Failed to delete {}{}{}", container, SEPARATOR, entry.getKey());
                    }
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.error("Failed to delete {}{}{}: {}", container, SEPARATOR, entry.getKey(), e);
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
        final String rootDir = getFolderOfKey(container, blobMetadata.getName());

        // Check if parent folder exists
        try {
            final RemoteFolder targetFolder = assureParentFolder(rootDir);
            PCloudMultipartUpload upload = this.multipartUploadFactory.create(targetFolder.folderId(), container, name,
                    uploadId, blobMetadata, options);
            upload.start();
            this.currentMultipartUploads.put(uploadId, upload);
            return upload;
        } catch (IOException | ApiError e) {
            throw new RuntimeException(e);
        }
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
