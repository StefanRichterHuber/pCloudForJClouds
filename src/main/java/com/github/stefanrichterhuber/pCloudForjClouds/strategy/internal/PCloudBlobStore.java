package com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.tryFind;
import static com.google.common.collect.Sets.newTreeSet;
import static com.google.common.io.BaseEncoding.base16;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.KeyNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.BlobBuilder.PayloadBlobBuilder;
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

import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudBlobKeyValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudContainerNameValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;
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

@Singleton
public final class PCloudBlobStore implements BlobStore {

	private static final String SEPARATOR = "/";
	private static final String MULTIPART_PREFIX = ".mpus-";
	private static final byte[] EMPTY_CONTENT = new byte[0];
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

	@Inject
	protected PCloudBlobStore( //
			BlobStoreContext context, Provider<BlobBuilder> blobBuilders, //
			@Named(PCloudConstants.PROPERTY_BASEDIR) String baseDir, //
			ApiClient apiClient, //
			PCloudContainerNameValidator pCloudContainerNameValidator, //
			PCloudBlobKeyValidator pCloudBlobKeyValidator, //
			@Memoized Supplier<Set<? extends Location>> locations, //
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
			throw new ContainerNotFoundException(container, String.format("Container %s not found in %s",
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
	 * Checks if all directory parts exists, and create them if necessary
	 * 
	 * @param rootDir
	 * @return {@link RemoteFolder} with all parents created
	 * @throws ApiError
	 * @throws IOException
	 */
	private RemoteFolder assureParentFolder(String rootDir) throws IOException, ApiError {
		final String[] parts = rootDir.split(SEPARATOR);
		/*
		 * Recursively check folder structure starting with base directory
		 */
		RemoteFolder remoteFolder = this.getApiClient().listFolder(this.createPath(), true).execute();
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
	 * @param key
	 * @return
	 * @return
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
	 * @param key
	 * @return
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
		byte[] content = IOUtils.toByteArray(blob.getPayload().openStream());
		return DataSource.create(content);
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
	private SortedSet<StorageMetadata> collectStorageMetadata(String container, final RemoteFolder remoteFolder,
			ListContainerOptions options) {
		final SortedSet<StorageMetadata> result = new TreeSet<>();
		for (RemoteEntry entry : remoteFolder.children()) {
			if (matchesOptions(entry, options)) {
				if (entry.isFile()) {
					Blob blob = this.createBlobFromRemoteFile(container, entry.asFile());
					result.add(blob.getMetadata());
				} else if (entry.isFolder()) {
					result.addAll(this.collectStorageMetadata(container, entry.asFolder(), options));
				}
			}
		}
		return result;
	}

	/**
	 * Checks if the given {@link RemoteFile} matches the given
	 * {@link ListContainerOptions}.
	 * 
	 * @param remoteFile
	 * @param options    {@link ListContainerOptions} to check {@link RemoteFile}
	 *                   against.
	 * @return
	 */
	@SuppressWarnings("deprecation")
	private boolean matchesOptions(RemoteFile remoteFile, ListContainerOptions options) {
		if (options == null || options == ListContainerOptions.NONE) {
			return true;
		}
		if (options.getPrefix() != null && !remoteFile.name().startsWith(options.getPrefix())) {
			return false;
		}
		if (options.getDelimiter() != null) {
			this.logger.debug("ListContainerOption 'delimiter' not supported");
		}
		if (options.getDir() != null) {
			this.logger.debug("ListContainerOption 'dir' not supported");
		}
		return true;
	}

	/**
	 * Checks if the given {@link RemoteFolder} matches the given
	 * {@link ListContainerOptions}.
	 * 
	 * @param remoteFolder
	 * @param options      {@link ListContainerOptions} to check
	 *                     {@link RemoteFolder} against.
	 * @return
	 */
	@SuppressWarnings("deprecation")
	private boolean matchesOptions(RemoteFolder remoteFolder, ListContainerOptions options) {
		if (options == null || options == ListContainerOptions.NONE) {
			return true;
		}
		if (!options.isRecursive()) {
			return false;
		}
		if (options.getDelimiter() != null) {
			this.logger.debug("ListContainerOption 'delimiter' not supported");
		}
		if (options.getDir() != null) {
			this.logger.debug("ListContainerOption 'dir' not supported");
		}
		return true;
	}

	/**
	 * Checks if the given {@link RemoteEntry} matches the given
	 * {@link ListContainerOptions}.
	 * 
	 * @param remoteEntry
	 * @param options     {@link ListContainerOptions} to check {@link RemoteEntry}
	 *                    against.
	 * @return
	 */
	private boolean matchesOptions(RemoteEntry remoteEntry, ListContainerOptions options) {
		if (options == null || options == ListContainerOptions.NONE) {
			return true;
		}
		if (remoteEntry.isFile()) {
			return matchesOptions(remoteEntry.asFile(), options);
		} else if (remoteEntry.isFolder()) {
			return matchesOptions(remoteEntry.asFolder(), options);
		}
		return true;
	}

	/**
	 * Utility method to clear all folders in the given {@link RemoteFolder}
	 * 
	 * @param folder  RemoteFolder to clear
	 * @param options {@link ListContainerOptions} to apply
	 * @throws IOException
	 */
	private void clearRemoteFolder(RemoteFolder folder, ListContainerOptions options) throws IOException {
		for (RemoteEntry child : folder.children()) {
			if (matchesOptions(child, options)) {
				if (child.isFile()) {
					child.delete();
				}
				if (child.isFolder()) {
					clearRemoteFolder(child.asFolder(), options);
				}
			}
		}
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

			return metadata;
		} catch (ApiError e) {
			final PCloudError pCloudError = PCloudError.parse(e);
			if (pCloudError == PCloudError.DIRECTORY_DOES_NOT_EXIST
					|| pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND) {
				logger.debug("Container {} not found: {}", container, pCloudError.getCode());
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
	private Blob createBlobFromRemoteFile(String container, final RemoteFile remoteFile) {
		final Map<String, String> userMetadata = new HashMap<>();
		userMetadata.put("parentFolderId", "" + remoteFile.parentFolderId());

		final Payload payload = remoteFile.size() == 0 || remoteFile.isFolder() ? new EmptyPayload()
				: new RemoteFilePayload(remoteFile);
		final Blob blob = this.blobBuilders.get() //
				.payload(payload) //
				.contentType(remoteFile.contentType()) //
				.contentLength(remoteFile.size()) //
				.eTag(remoteFile.hash()) //
				.name(remoteFile.name()) //
				.userMetadata(userMetadata) //
				.type(remoteFile.isFolder() ? StorageType.FOLDER : StorageType.BLOB) //
				.build();
		blob.getMetadata().setLastModified(remoteFile.lastModified());
		blob.getMetadata().setCreationDate(remoteFile.created());
		blob.getMetadata().setId(remoteFile.id());
		blob.getMetadata().setContainer(container);
		return blob;
	}

	private long countBlobs(RemoteFolder folder, ListContainerOptions options) {
		if (folder != null) {
			long count = 0;
			for (RemoteEntry child : folder.children()) {
				if (matchesOptions(child, options)) {
					if (child.isFile()) {
						count++;
					}
					if (child.isFolder()) {
						count += countBlobs(child.asFolder(), options);
					}
				}
				if (count >= options.getMaxResults()) {
					return count;
				}
			}
			return count;
		}
		return 0l;
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
		try {
			final String path = createPath(container);
			RemoteFolder remoteFolder = this.getApiClient().listFolder(path).execute();
			return remoteFolder.isFolder();
		} catch (ApiError e) {
			final PCloudError pCloudError = PCloudError.parse(e);
			if (pCloudError == PCloudError.DIRECTORY_DOES_NOT_EXIST
					|| pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND) {
				logger.debug("Container {} not found: {}", container, pCloudError.getCode());
				return false;
			}
			throw new PCloudBlobStoreException(e);
		} catch (IOException e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public boolean createContainerInLocation(Location location, String container) {
		this.pCloudContainerNameValidator.validate(container);
		try {
			String path = createPath(container);
			RemoteFolder remoteFolder = this.getApiClient().createFolder(path).execute();
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

	@Override
	public PageSet<? extends StorageMetadata> list(String container) {
		return this.list(container, ListContainerOptions.NONE);

	}

	@SuppressWarnings("deprecation")
	@Override
	public PageSet<? extends StorageMetadata> list(String containerName, ListContainerOptions options) {
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
			RemoteFolder remoteFolder = this.getApiClient().listFolder(createPath(containerName)).execute();
			SortedSet<StorageMetadata> contents = collectStorageMetadata(containerName, remoteFolder, options);

			String marker = null;
			if (options != null) {
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
	public void clearContainer(String container) {
		clearContainer(container, ListContainerOptions.Builder.recursive());

	}

	@Override
	public void clearContainer(String container, ListContainerOptions options) {
		this.pCloudContainerNameValidator.validate(container);

		final boolean recursivly = options.isRecursive();

		try {
			final RemoteFolder folder = this.getApiClient().listFolder(this.createPath(container), recursivly)
					.execute();
			clearRemoteFolder(folder, options);
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
			return true;
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public boolean directoryExists(String container, String directory) {
		assertContainerExists(container);
		try {
			return this.getApiClient().listFolder(createPath(container, directory)).execute().isFolder();
		} catch (IOException | ApiError e) {
			if (e instanceof ApiError && PCloudError.parse((ApiError) e) == PCloudError.DIRECTORY_DOES_NOT_EXIST) {
				return false;
			}
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public void createDirectory(String container, String directory) {
		assertContainerExists(container);
		this.pCloudBlobKeyValidator.validate(directory);
		try {
			this.getApiClient().createFolder(this.createPath(container, directory)).execute();
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}

	}

	@Override
	public void deleteDirectory(String containerName, String name) {
		assertContainerExists(containerName);
		try {
			this.getApiClient().deleteFolder(createPath(containerName, name)).execute();
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
				logger.debug("Blob {}/{} not found: {}", container, key, pCloudError.getCode());
				return false;
			}
			throw new PCloudBlobStoreException(e);
		} catch (IOException e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public String putBlob(String container, Blob blob) {
		return putBlob(container, blob, PutOptions.NONE);
	}

	@Override
	public String putBlob(String container, Blob blob, PutOptions options) {
		assertContainerExists(container);
		this.pCloudBlobKeyValidator.validate(blob.getMetadata().getName());

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
					UploadOptions.OVERRIDE_FILE);
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
	public BlobMetadata blobMetadata(String container, String name) {
		Blob blob = getBlob(container, name, null);
		return blob != null ? blob.getMetadata() : null;
	}

	@Override
	public Blob getBlob(String container, String name) {
		return getBlob(container, name, GetOptions.NONE);
	}

	@Override
	public Blob getBlob(String container, String name, GetOptions options) {
		assertContainerExists(container);
		try {
			final RemoteFile remoteFile = this.getApiClient().loadFile(createPath(container, name)).execute();
			final Blob blob = createBlobFromRemoteFile(container, remoteFile);

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
				logger.debug("Blob {}/{} not found: {}", container, name, pCloudError.getCode());
				return null;
			}
			if (pCloudError == PCloudError.PARENT_DIR_DOES_NOT_EXISTS) {
				logger.warn("Parent folder of blob {}/{} does not exist", container, name);
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

	@Override
	public long countBlobs(String container) {
		return countBlobs(container, ListContainerOptions.Builder.recursive());
	}

	@Override
	public long countBlobs(String container, ListContainerOptions options) {
		assertContainerExists(container);
		try {
			final RemoteFolder remoteFolder = this.getApiClient().listFolder(createPath(container)).execute();
			return this.countBlobs(remoteFolder, options);
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public MultipartUpload initiateMultipartUpload(String container, BlobMetadata blobMetadata, PutOptions options) {
		assertContainerExists(container);
		String uploadId = UUID.randomUUID().toString();
		// create a stub blob
		Blob blob = blobBuilder(MULTIPART_PREFIX + uploadId + "-" + blobMetadata.getName() + "-stub")
				.payload(ByteSource.empty()).build();
		putBlob(container, blob);
		return MultipartUpload.create(container, blobMetadata.getName(), uploadId, blobMetadata, options);
	}

	@Override
	public void abortMultipartUpload(MultipartUpload mpu) {
		List<MultipartPart> parts = listMultipartUpload(mpu);
		List<String> keys = new ArrayList<>(
				parts.stream().map(part -> MULTIPART_PREFIX + mpu.id() + "-" + mpu.blobName() + "-" + part.partNumber())
						.collect(Collectors.toList()));
		keys.add(MULTIPART_PREFIX + mpu.id() + "-" + mpu.blobName() + "-stub");
		this.removeBlobs(mpu.containerName(), keys);
	}

	@Override
	public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
		ImmutableList.Builder<Blob> blobs = ImmutableList.builder();
		long contentLength = 0;
		Hasher md5Hasher = Hashing.md5().newHasher();

		for (MultipartPart part : parts) {
			Blob blobPart = getBlob(mpu.containerName(),
					MULTIPART_PREFIX + mpu.id() + "-" + mpu.blobName() + "-" + part.partNumber());
			contentLength += blobPart.getMetadata().getContentMetadata().getContentLength();
			blobs.add(blobPart);
			if (blobPart.getMetadata().getETag() != null) {
				md5Hasher.putBytes(BaseEncoding.base16().lowerCase().decode(blobPart.getMetadata().getETag()));
			}
		}
		String mpuETag = new StringBuilder("\"").append(md5Hasher.hash()).append("-").append(parts.size()).append("\"")
				.toString();
		PayloadBlobBuilder blobBuilder = blobBuilder(mpu.blobName()).userMetadata(mpu.blobMetadata().getUserMetadata())
				.payload(new MultiBlobInputStream(blobs.build())).contentLength(contentLength).eTag(mpuETag);
		String cacheControl = mpu.blobMetadata().getContentMetadata().getCacheControl();
		if (cacheControl != null) {
			blobBuilder.cacheControl(cacheControl);
		}
		String contentDisposition = mpu.blobMetadata().getContentMetadata().getContentDisposition();
		if (contentDisposition != null) {
			blobBuilder.contentDisposition(contentDisposition);
		}
		String contentEncoding = mpu.blobMetadata().getContentMetadata().getContentEncoding();
		if (contentEncoding != null) {
			blobBuilder.contentEncoding(contentEncoding);
		}
		String contentLanguage = mpu.blobMetadata().getContentMetadata().getContentLanguage();
		if (contentLanguage != null) {
			blobBuilder.contentLanguage(contentLanguage);
		}
		// intentionally not copying MD5
		String contentType = mpu.blobMetadata().getContentMetadata().getContentType();
		if (contentType != null) {
			blobBuilder.contentType(contentType);
		}
		Date expires = mpu.blobMetadata().getContentMetadata().getExpires();
		if (expires != null) {
			blobBuilder.expires(expires);
		}
		Tier tier = mpu.blobMetadata().getTier();
		if (tier != null) {
			blobBuilder.tier(tier);
		}

		putBlob(mpu.containerName(), blobBuilder.build());

		for (MultipartPart part : parts) {
			removeBlob(mpu.containerName(),
					MULTIPART_PREFIX + mpu.id() + "-" + mpu.blobName() + "-" + part.partNumber());
		}
		removeBlob(mpu.containerName(), MULTIPART_PREFIX + mpu.id() + "-" + mpu.blobName() + "-stub");

		setBlobAccess(mpu.containerName(), mpu.blobName(), mpu.putOptions().getBlobAccess());

		return mpuETag;
	}

	@Override
	public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {
		String partName = MULTIPART_PREFIX + mpu.id() + "-" + mpu.blobName() + "-" + partNumber;
		Blob blob = blobBuilder(partName).payload(payload).build();
		String partETag = putBlob(mpu.containerName(), blob);
		BlobMetadata metadata = blobMetadata(mpu.containerName(), partName);
		long partSize = metadata.getContentMetadata().getContentLength();
		return MultipartPart.create(partNumber, partSize, partETag, metadata.getLastModified());
	}

	@Override
	public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
		ImmutableList.Builder<MultipartPart> parts = ImmutableList.builder();
		ListContainerOptions options = new ListContainerOptions()
				.prefix(MULTIPART_PREFIX + mpu.id() + "-" + mpu.blobName() + "-").recursive();
		while (true) {
			PageSet<? extends StorageMetadata> pageSet = list(mpu.containerName(), options);
			for (StorageMetadata sm : pageSet) {
				if (sm.getName().endsWith("-stub")) {
					continue;
				}
				int partNumber = Integer.parseInt(
						sm.getName().substring((MULTIPART_PREFIX + mpu.id() + "-" + mpu.blobName() + "-").length()));
				long partSize = sm.getSize();
				parts.add(MultipartPart.create(partNumber, partSize, sm.getETag(), sm.getLastModified()));
			}
			if (pageSet.isEmpty() || pageSet.getNextMarker() == null) {
				break;
			}
			options.afterMarker(pageSet.getNextMarker());
		}
		return parts.build();
	}

	@Override
	public List<MultipartUpload> listMultipartUploads(String container) {
		assertContainerExists(container);
		ImmutableList.Builder<MultipartUpload> mpus = ImmutableList.builder();
		ListContainerOptions options = new ListContainerOptions().prefix(MULTIPART_PREFIX).recursive();
		int uuidLength = UUID.randomUUID().toString().length();
		while (true) {
			PageSet<? extends StorageMetadata> pageSet = list(container, options);
			for (StorageMetadata sm : pageSet) {
				if (!sm.getName().endsWith("-stub")) {
					continue;
				}
				String uploadId = sm.getName().substring(MULTIPART_PREFIX.length(),
						MULTIPART_PREFIX.length() + uuidLength);
				String blobName = sm.getName().substring(MULTIPART_PREFIX.length() + uuidLength + 1);
				int index = blobName.lastIndexOf('-');
				blobName = blobName.substring(0, index);

				mpus.add(MultipartUpload.create(container, blobName, uploadId, null, null));
			}
			if (pageSet.isEmpty() || pageSet.getNextMarker() == null) {
				break;
			}
			options.afterMarker(pageSet.getNextMarker());
		}

		return mpus.build();
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

	@Override
	public void downloadBlob(String container, String name, File destination) {
		try (InputStream ips = this.streamBlob(container, name); OutputStream fos = new FileOutputStream(destination)) {
			IOUtils.copy(ips, fos);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}

	@Override
	public void downloadBlob(String container, String name, File destination, ExecutorService executor) {
		this.downloadBlob(container, name, destination);

	}

	@Override
	public InputStream streamBlob(String container, String name) {
		final Blob blob = this.getBlob(container, name);
		if (blob != null && blob.getPayload() != null) {
			try {
				return blob.getPayload().openStream();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	@Override
	public InputStream streamBlob(String container, String name, ExecutorService executor) {
		return this.streamBlob(container, name);
	}

	private static final class MultiBlobInputStream extends InputStream {
		private final Iterator<Blob> blobs;
		private InputStream current;

		MultiBlobInputStream(List<Blob> blobs) {
			this.blobs = blobs.iterator();
		}

		@Override
		public int read() throws IOException {
			byte[] b = new byte[1];
			int result = read(b, 0, b.length);
			if (result == -1) {
				return -1;
			}
			return b[0] & 0x000000FF;
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			while (true) {
				if (current == null) {
					if (!blobs.hasNext()) {
						return -1;
					}
					current = blobs.next().getPayload().openStream();
				}
				int result = current.read(b, off, len);
				if (result == -1) {
					current.close();
					current = null;
					continue;
				}
				return result;
			}
		}
	}
}
