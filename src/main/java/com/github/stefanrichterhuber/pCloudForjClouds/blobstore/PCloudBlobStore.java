package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.tryFind;
import static com.google.common.collect.Sets.newTreeSet;
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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.annotation.Resource;
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
import org.jclouds.logging.Logger;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.AbstractBlobStore;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.BlobDataSource;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudMultipartUpload;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudBlobKeyValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudContainerNameValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal.EmptyPayload;
import com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal.PCloudBlobStoreException;
import com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal.PCloudError;
import com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal.RemoteFilePayload;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteSource;
import com.google.common.net.HttpHeaders;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.Call;
import com.pcloud.sdk.Callback;
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
public final class PCloudBlobStore extends AbstractBlobStore implements BlobStore {

	private static final String SEPARATOR = "/";
	private static final byte[] EMPTY_CONTENT = new byte[0];
	@SuppressWarnings("deprecation")
	private static final byte[] DIRECTORY_MD5 = Hashing.md5().hashBytes(EMPTY_CONTENT).asBytes();

	@Resource
	protected Logger logger = Logger.NULL;

	private final Provider<BlobBuilder> blobBuilders;
	private final String baseDirectory;
	private final PCloudContainerNameValidator pCloudContainerNameValidator;
	private final PCloudBlobKeyValidator pCloudBlobKeyValidator;
	private final Supplier<Location> defaultLocation;
	private final ApiClient apiClient;
	private final BlobStoreContext context;
	private final Supplier<Set<? extends Location>> locations;
	private final MultipartUploadFactory multipartUploadFactory;
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
		RemoteFolder baseFolder = this.createBaseDirectory();
		// Directly add the folder to the cache
		this.remoteFolderCache.put(baseDirectory, baseFolder);
	}

	/**
	 * Creates the root folder for the
	 */
	private RemoteFolder createBaseDirectory() {
		try {
			RemoteFolder remoteFolder = this.apiClient.loadFolder(this.baseDirectory).execute();
			if (remoteFolder != null) {
				return remoteFolder;
			}
		} catch (IOException | ApiError e) {
			// Ignore - folder does not exist
		}

		// Folder does not exist - create it
		final String[] parts = baseDirectory.split(SEPARATOR);
		/*
		 * Recursively check folder structure starting with base directory
		 */
		try {
			RemoteFolder remoteFolder = this.getApiClient().listFolder("/", true).execute();

			for (int i = 0; i < parts.length; i++) {
				final int idx = i;
				final String subFolder = parts[idx];

				RemoteFolder nxt = remoteFolder.children().stream()
						.filter(re -> re.isFolder() && re.name().equals(subFolder)).findFirst().map(re -> re.asFolder())
						.orElse(null);
				if (nxt == null) {
					// Remote folder not found -> create it and all necessary children
					nxt = this.getApiClient().createFolder(remoteFolder.folderId(), subFolder).execute();
				}
				remoteFolder = nxt;
			}
			return remoteFolder;
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}

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
			logger.warn("Container %s does not exist", container);
			throw new ContainerNotFoundException(container, String.format("Container %s not found in %s", container,
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
			logger.warn("Blob %s does not exist in container %s", name, container);
			throw new KeyNotFoundException(container, name, "Not found");
		}
	}

	/**
	 * Checks if the directory exsits, and creates it if not.
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
				this.logger.info("Parent folder %s with id %d created!", dir, rf.folderId());
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
					this.logger.debug("Parent folder %s with id %d exists in cache and remote.", dir, v.folderId());
					return remoteFolder;
				}
			} catch (IOException | ApiError e) {
				this.logger.debug("Parent folder %s with id %d exists in cache but not remote!", dir, v.folderId());
				// Folder does not exist anymore
				return null;
			}
		}
		// Folder not in cache, check if present remote
		try {
			RemoteFolder remoteFolder = this.apiClient.listFolder(this.createPath(dir)).execute();
			this.logger.debug("Parent folder %s exists only remote, but not yet in cache.", dir);
			return remoteFolder;
		} catch (IOException | ApiError e) {
			this.logger.debug("Parent folder %s does not exist.", dir);
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
	private static String stripDirectorySuffix(String key) {
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
	private static DataSource dataSourceFromBlob(Blob blob) throws IOException {
		return new BlobDataSource(blob);
	}

	/**
	 * Collects all {@link StorageMetadata} from the given {@link RemoteFolder} in
	 * the given container
	 * 
	 * @param container    Container containing all entries
	 * @param remoteFolder {@link RemoteFolder} to check
	 * @param options      {@link ListContainerOptions} to apply
	 * @return
	 */
	private SortedSet<StorageMetadata> collectStorageMetadata(String container, String parentFolder,
			final RemoteFolder remoteFolder, ListContainerOptions options) {
		final SortedSet<StorageMetadata> result = new TreeSet<>();
		for (RemoteEntry entry : remoteFolder.children()) {
			// Files have a key like folder1/folder2/file, folders like
			// folder1/folder2/folder/
			final String key = parentFolder != null ? parentFolder + entry.name()
					: entry.name() + (entry.isFolder() ? SEPARATOR : "");
			if (matchesOptions(key, entry, options)) {
				if (entry.isFile()) {
					final Blob blob = this.createBlobFromRemoteEntry(container, key, entry.asFile());
					result.add(blob.getMetadata());
				} else if (entry.isFolder()) {
					final Blob blob = this.createBlobFromRemoteEntry(container, key, entry.asFolder());
					result.add(blob.getMetadata());
				}
			}
			if (options.isRecursive() && entry.isFolder()) {
				result.addAll(this.collectStorageMetadata(container, key, entry.asFolder(), options));
			}

		}
		return result;
	}

	/**
	 * Checks if the given {@link RemoteEntry} matches the given
	 * {@link ListContainerOptions}.
	 * 
	 * @param remoteEntry {@link RemoteEntry} to match
	 * @param key         Key of the {@link RemoteEntry} to match.
	 * @param options     {@link ListContainerOptions} to check {@link RemoteEntry}
	 *                    against.
	 * @return
	 */
	@SuppressWarnings("deprecation")
	private boolean matchesOptions(String key, RemoteEntry remoteEntry, ListContainerOptions options) {
		if (options == null || options == ListContainerOptions.NONE) {
			return true;
		}
		if (options.getPrefix() != null && !key.startsWith(options.getPrefix())) {
			return false;
		}
		if (remoteEntry.isFolder() && !options.isRecursive()) {
			return false;
		}
		if (options.getDelimiter() != null) {
			this.logger.warn("ListContainerOption 'delimiter' not supported");
		}
		if (options.getDir() != null) {
			this.logger.warn("ListContainerOption 'dir' not supported");
		}
		return true;
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

			logger.debug("Requested storage metadata for container %s: %s", container, metadata);

			return metadata;
		} catch (ApiError e) {
			final PCloudError pCloudError = PCloudError.parse(e);
			if (pCloudError == PCloudError.DIRECTORY_DOES_NOT_EXIST
					|| pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND) {
				logger.debug("Container %s not found: %s", container, pCloudError.getCode());
				return null;
			}
			throw new PCloudBlobStoreException(e);
		} catch (IOException e) {
			throw new PCloudBlobStoreException(e);
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
	 * @param container  Host container of the file
	 * @param remoteFile {@link RemoteFile}.
	 * @return {@link Blob} created
	 */
	private Blob createBlobFromRemoteEntry(String container, String key, RemoteEntry remoteFile) {
		Blob blob = null;
		final Map<String, String> userMetadata = new HashMap<>();
		userMetadata.put("parentFolderId", "" + remoteFile.parentFolderId());

		if (remoteFile instanceof RemoteFile && remoteFile.isFile()) {
			final RemoteFile file = (RemoteFile) remoteFile;
			final Payload payload = file.size() == 0 ? new EmptyPayload() : new RemoteFilePayload(file);
			blob = this.blobBuilders.get() //
					.payload(payload) //
					.contentType(file.contentType()) //
					.contentLength(file.size()) //
					.eTag(file.hash()) //
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

	/**
	 * Utility function to execute a {@link Call} async and wrap its results in a
	 * {@link CompletableFuture}.
	 * 
	 * @param <T>
	 * @param call {@link Call} to execute and wrap
	 * @return {@link CompletableFuture} containing the result of the {@link Call}.
	 */
	private static <T> CompletableFuture<T> execute(Call<T> call) {
		final CompletableFuture<T> result = new CompletableFuture<T>();

		call.enqueue(new Callback<T>() {

			@Override
			public void onResponse(Call<T> call, T response) {
				result.complete(response);
			}

			@Override
			public void onFailure(Call<T> call, Throwable t) {
				result.completeExceptionally(t);
			}
		});
		return result;
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
		logger.info("Requested storage metadata for all containers");

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
		return remoteFolder != null;
	}

	@Override
	public boolean createContainerInLocation(Location location, String container) {
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
		this.logger.debug("Retrieving access rights for PCloud containers not supported: %s", container);
		return ContainerAccess.PRIVATE;
	}

	@Override
	public void setContainerAccess(String container, ContainerAccess access) {
		this.pCloudContainerNameValidator.validate(container);
		this.logger.debug("Settings access rights for PCloud containers not supported: %s", container);

	}

	@SuppressWarnings("deprecation")
	@Override
	public PageSet<? extends StorageMetadata> list(String containerName, ListContainerOptions options) {
		logger.info("Requested storage metadata for container %s with options %s", containerName, options);
		if (options.getDir() != null && options.getPrefix() != null) {
			throw new IllegalArgumentException("Cannot set both prefix and directory");
		}

		if ((options.getDir() != null || options.isRecursive()) && (options.getDelimiter() != null)) {
			throw new IllegalArgumentException("Cannot set the delimiter if directory or recursive is set");
		}

		// Check if the container exists
		assertContainerExists(containerName);

		try {
			// Loading blobs from container
			RemoteFolder remoteFolder = this.getApiClient().listFolder(createPath(containerName), options.isRecursive())
					.execute();
			SortedSet<StorageMetadata> contents = collectStorageMetadata(containerName, null, remoteFolder, options);

			String marker = null;
			if (options != null && options != ListContainerOptions.NONE) {
				if (options.getMarker() != null) {
					final String finalMarker = options.getMarker();
					Optional<StorageMetadata> lastMarkerMetadata = tryFind(contents, new Predicate<StorageMetadata>() {
						public boolean apply(StorageMetadata metadata) {
							return metadata.getName().compareTo(finalMarker) > 0;
						}
					});
					if (lastMarkerMetadata.isPresent()) {
						contents = contents.tailSet(lastMarkerMetadata.get());
					} else {
						// marker is after last key or container is empty
						contents.clear();
					}
				}

				int maxResults = options.getMaxResults() != null ? options.getMaxResults() : 1000;
				if (!contents.isEmpty()) {
					StorageMetadata lastElement = contents.last();
					contents = newTreeSet(Iterables.limit(contents, maxResults));
					if (maxResults != 0 && !contents.contains(lastElement)) {
						// Partial listing
						lastElement = contents.last();
						marker = lastElement.getName();
					}
				}

				// trim metadata, if the response isn't supposed to be detailed.
				if (!options.isDetailed()) {
					for (StorageMetadata md : contents) {
						md.getUserMetadata().clear();
					}
				}
			}

			return new PageSetImpl<StorageMetadata>(contents, marker);
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public void deleteContainer(String container) {
		assertContainerExists(container);
		try {
			final RemoteFolder folder = this.getApiClient().listFolder(createPath(container)).execute();
			folder.delete(true);
			// TODO invalidate cache
			this.logger.debug("Successfully deleted container %s", container);
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}

	}

	@Override
	public boolean deleteContainerIfEmpty(String container) {
		assertContainerExists(container);
		try {
			final RemoteFolder folder = this.getApiClient().listFolder(createPath(container)).execute();
			folder.delete(true);
			this.logger.debug("Successfully deleted container %s", container);
			// TODO invalidate cache
			return true;
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public boolean directoryExists(String container, String directory) {
		assertContainerExists(container);
		directory = stripDirectorySuffix(directory);
		RemoteFolder remoteFolder = this.remoteFolderCache.compute(container + SEPARATOR + directory,
				this::checkOrFetchFolder);
		return remoteFolder != null;
	}

	@Override
	public void createDirectory(String container, String directory) {
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
		assertContainerExists(containerName);
		directory = stripDirectorySuffix(directory);
		try {
			this.getApiClient().deleteFolder(createPath(containerName, directory)).execute();
			// TODO invalidate cache
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}

	}

	@Override
	public boolean blobExists(String container, String key) {
		assertContainerExists(container);
		this.pCloudBlobKeyValidator.validate(key);

		final String name = getFileOfKey(key);

		final String rootDir = getFolderOfKey(container, key);

		try {
			final RemoteFolder remoteFolder = this.getApiClient().listFolder(this.createPath(rootDir)).execute();
			return remoteFolder.children().stream().anyMatch(re -> re.name().equals(name));
		} catch (ApiError e) {
			final PCloudError pCloudError = PCloudError.parse(e);
			if (pCloudError == PCloudError.DIRECTORY_DOES_NOT_EXIST
					|| pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND) {
				logger.debug("Blob %s/%s not found: %s", container, key, pCloudError.getCode());
				return false;
			}
			throw new PCloudBlobStoreException(e);
		} catch (IOException e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public String putBlob(String container, Blob blob, PutOptions options) {
		assertContainerExists(container);
		this.pCloudBlobKeyValidator.validate(blob.getMetadata().getName());

		logger.debug("Uploading blob %s to container %s", container, blob);
		try {
			if (getDirectoryBlobSuffix(blob.getMetadata().getName()) != null) {
				return putDirectoryBlob(container, blob);
			}

			final String name = getFileOfKey(blob.getMetadata().getName());

			final String rootDir = getFolderOfKey(container, blob.getMetadata().getName());

			// Check if parent folder exists
			final RemoteFolder targetFolder = assureParentFolder(rootDir);

			final DataSource ds = dataSourceFromBlob(blob);
			final Call<RemoteFile> createdFile = this.getApiClient().createFile(targetFolder, name, ds,
					blob.getMetadata().getLastModified(), null, UploadOptions.OVERRIDE_FILE);
			RemoteFile remoteFile = createdFile.execute();
			return remoteFile.hash();
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

			if (source.getPayload().getRawContent() instanceof RemoteFile) {

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
				}

				if (result != null) {
					return result.isFile() ? result.asFile().hash() : base16().lowerCase().encode(DIRECTORY_MD5);
				} else {
					return null;
				}
			} else {
				// The source is no real file
				throw new KeyNotFoundException(fromContainer, fromName,
						"Source not found / is not a real file during copy");
			}

		} catch (ApiError e) {
			final PCloudError pCloudError = PCloudError.parse(e);
			if (pCloudError == PCloudError.FILE_NOT_FOUND || pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND) {
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
			// Remove final directory separator when fetching a directory
			name = stripDirectorySuffix(name);

			final RemoteFile remoteFile = this.getApiClient().loadFile(createPath(container, name)).execute();
			final Blob blob = createBlobFromRemoteEntry(container, name, remoteFile);

			if (options != null && options != GetOptions.NONE) {
				final String eTag = maybeQuoteETag(remoteFile.hash());
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
		} catch (ApiError e) {
			final PCloudError pCloudError = PCloudError.parse(e);
			if (pCloudError == PCloudError.FILE_NOT_FOUND || pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND) {
				logger.debug("Blob %s/%s not found: %s", container, name, pCloudError.getCode());
				return null;
			}
			if (pCloudError == PCloudError.PARENT_DIR_DOES_NOT_EXISTS) {
				logger.warn("Parent folder of blob %s/%s does not exist", container, name);
				return null;
			}
			throw new PCloudBlobStoreException(e);
		} catch (IOException e) {
			throw new PCloudBlobStoreException(e);
		}

	}

	@Override
	public void removeBlob(String container, String name) {
		this.pCloudContainerNameValidator.validate(container);
		try {
			final Boolean result = execute(this.getApiClient().deleteFile(this.createPath(container, name))).get();
			if (!result) {
				logger.error("Failed to delete %s%s%s", container, SEPARATOR, name);
			}
		} catch (InterruptedException | ExecutionException e) {
			logger.warn("Failed to delete %s%s%s: %s", container, SEPARATOR, name, e);
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public void removeBlobs(String container, Iterable<String> names) {
		assertContainerExists(container);
		if (names != null) {
			// Start all remove jobs in parallel
			final Map<String, CompletableFuture<Boolean>> jobs = new HashMap<>();
			for (String name : names) {
				jobs.put(name, execute(this.getApiClient().deleteFile(createPath(container, name))));
			}
			for (Entry<String, CompletableFuture<Boolean>> entry : jobs.entrySet()) {
				try {
					if (entry.getValue().get()) {
						logger.debug("Successfully deleted %s%s%s", container, entry.getKey());
					} else {
						logger.error("Failed to delete %s%s%s", container, SEPARATOR, entry.getKey());
					}
				} catch (InterruptedException | ExecutionException e) {
					logger.error("Failed to delete %s%s%s: %s", container, SEPARATOR, entry.getKey(), e);
				}
			}
		}
	}

	@Override
	public BlobAccess getBlobAccess(String container, String name) {
		assertContainerExists(container);
		this.logger.debug("Retrieving access rights for PCloud blobs not supported: %s%s%s", container, SEPARATOR,
				name);
		return BlobAccess.PRIVATE;
	}

	@Override
	public void setBlobAccess(String container, String name, BlobAccess access) {
		assertContainerExists(container);
		assertBlobExists(container, name);
		this.logger.debug("Setting access rights for PCloud blobs not supported: %s%s%s", container, SEPARATOR, name);
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
		resolveUpload(mpu).abort();
		this.currentMultipartUploads.remove(mpu.id());
	}

	@Override
	public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
		final String etag = resolveUpload(mpu).complete();
		this.currentMultipartUploads.remove(mpu.id());
		return etag;
	}

	@Override
	public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {
		try {
			return resolveUpload(mpu).append(partNumber, payload);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
		return resolveUpload(mpu).getParts();
	}

	@Override
	public List<MultipartUpload> listMultipartUploads(String container) {

		final List<MultipartUpload> result = new ArrayList<>(this.currentMultipartUploads.values().stream()
				.filter(v -> v.containerName().equals(container)).collect(Collectors.toList()));
		if (result.size() > 0) {
			logger.debug("Found active multipart uploads for container %s: %s", container, result);
		}
		return result;
	}

	@Override
	public long getMinimumMultipartPartSize() {
		return 1;
	}

	@Override
	public long getMaximumMultipartPartSize() {
		return 5 * 1024 * 1024;
	}

	@Override
	public int getMaximumNumberOfParts() {
		return Integer.MAX_VALUE;
	}

	/**
	 * Removes all folders / blobs matching the given {@link ListContainerOptions}
	 * from the given folder.
	 * 
	 * @param parentFolder
	 * @param remoteFolder
	 * @param options
	 * @throws IOException
	 */
	private void clearContainer(String parentFolder, RemoteFolder remoteFolder, ListContainerOptions options)
			throws IOException {
		for (RemoteEntry entry : remoteFolder.children()) {
			// Files have a key like folder1/folder2/file, folders like
			// folder1/folder2/folder/
			final String key = parentFolder != null ? parentFolder + entry.name()
					: entry.name() + (entry.isFolder() ? SEPARATOR : "");
			if (matchesOptions(key, entry, options)) {
				entry.delete();
			} else if (options.isRecursive() && entry.isFolder()) {
				// Check recursively for further targets
				clearContainer(key, entry.asFolder(), options);
			}
		}
	}

	@Override
	public void clearContainer(String container, ListContainerOptions options) {
		try {
			final RemoteFolder remoteFolder = this.getApiClient()
					.listFolder(createPath(container), options.isRecursive()).execute();
			clearContainer(null, remoteFolder, options);
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}

	}
}
