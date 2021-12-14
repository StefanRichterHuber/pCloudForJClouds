package com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.BaseEncoding.base16;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.jclouds.blobstore.LocalStorageStrategy;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.ContainerAccess;
import org.jclouds.blobstore.domain.MutableStorageMetadata;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.domain.internal.MutableStorageMetadataImpl;
import org.jclouds.blobstore.options.CreateContainerOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.reference.BlobStoreConstants;
import org.jclouds.domain.Location;
import org.jclouds.io.Payload;
import org.jclouds.logging.Logger;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.BlobDataSource;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudBlobKeyValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudContainerNameValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.base.Supplier;
import com.google.common.hash.Hashing;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.RemoteEntry;
import com.pcloud.sdk.RemoteFile;
import com.pcloud.sdk.RemoteFolder;
import com.pcloud.sdk.UploadOptions;

public class PCloudStorageStrategyImpl implements LocalStorageStrategy {
	@Resource
	protected Logger logger = Logger.NULL;

	private static final String SEPARATOR = "/";

	private final Provider<BlobBuilder> blobBuilders;
	private final String baseDirectory;
	private final PCloudContainerNameValidator pCloudContainerNameValidator;
	private final PCloudBlobKeyValidator pCloudBlobKeyValidator;
	private final Supplier<Location> defaultLocation;
	private final ApiClient apiClient;

	private static final byte[] EMPTY_CONTENT = new byte[0];

	private static final byte[] DIRECTORY_MD5 = Hashing.md5().hashBytes(EMPTY_CONTENT).asBytes();

	@Inject
	protected PCloudStorageStrategyImpl( //
			Provider<BlobBuilder> blobBuilders, //
			@Named(PCloudConstants.PROPERTY_BASEDIR) String baseDir, //
			ApiClient apiClient, //
			PCloudContainerNameValidator pCloudContainerNameValidator, //
			PCloudBlobKeyValidator pCloudBlobKeyValidator, //
			Supplier<Location> defaultLocation //
	) {
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
			return this.baseDirectory + this.getSeparator()
					+ Arrays.asList(content).stream().collect(Collectors.joining(this.getSeparator()));
		} else {
			return this.baseDirectory;
		}
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

	private SortedSet<String> getBlobKeysInsideContainer(RemoteFolder remoteFolder, String prefix) throws IOException {
		final SortedSet<String> result = new TreeSet<>();
		for (RemoteEntry entry : remoteFolder.children()) {
			if (entry.isFile()) {
				if (prefix != null && entry.asFile().name().startsWith(prefix)) {
					result.add(entry.asFile().name());
				} else {
					result.add(entry.asFile().name());
				}
			} else if (entry.isFolder()) {
				result.addAll(this.getBlobKeysInsideContainer(entry.asFolder(), prefix));
			}
		}
		return result;
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
	 * Checks if all directory parts exists, and create them if necessary
	 * 
	 * @param rootDir
	 * @throws ApiError
	 * @throws IOException
	 */
	private RemoteFolder checkAndCreateParentFolder(String rootDir) throws IOException, ApiError {
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
	 * Creates a directory blob (simply a new folder within the container)
	 * 
	 * @param conainerName Name of the container
	 * @param blob         Directory blob
	 * @return Etag of the blob (always null!)
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

	@Override
	public boolean containerExists(String container) {
		this.pCloudContainerNameValidator.validate(container);
		try {
			final String path = createPath(container);
			final RemoteFolder remoteFolder = this.getApiClient().listFolder(path).execute();
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
	public Collection<String> getAllContainerNames() {
		try {
			final RemoteFolder rootFolder = this.getApiClient().listFolder(this.baseDirectory).execute();
			final List<String> result = rootFolder.children().stream().filter(re -> re.isFolder()).map(re -> re.name())
					.collect(Collectors.toList());
			return result;
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public boolean createContainerInLocation(String container, Location location, CreateContainerOptions options) {
		return this.createContainerInLocation(this.getLocation(container), container);
	}

	private boolean createContainerInLocation(Location location, String container) {
		this.pCloudContainerNameValidator.validate(container);
		try {
			final String path = createPath(container);
			final RemoteFolder remoteFolder = this.getApiClient().createFolder(path).execute();
			return remoteFolder.isFolder();
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
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
	public void deleteContainer(String container) {
		this.pCloudContainerNameValidator.validate(container);
		try {
			final RemoteFolder folder = this.getApiClient().listFolder(createPath(container)).execute();
			folder.delete(true);
			this.logger.debug("Successfully deleted container %s", container);
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
			for (RemoteEntry child : folder.children()) {
				child.delete();
			}
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public StorageMetadata getContainerMetadata(String container) {
		this.pCloudContainerNameValidator.validate(container);

		try {
			RemoteFolder remoteFolder = this.getApiClient().loadFolder(this.createPath(container)).execute();

			MutableStorageMetadata metadata = new MutableStorageMetadataImpl();
			metadata.setName(container);
			metadata.setType(StorageType.CONTAINER);
			metadata.setLocation(getLocation(container));
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

	@Override
	public boolean blobExists(String container, String key) {
		this.pCloudContainerNameValidator.validate(container);
		this.pCloudBlobKeyValidator.validate(key);

		final String name = getFileOfKey(key);

		final String rootDir = getFolderOfKey(container, key);

		try {
			final RemoteFolder remoteFolder = this.getApiClient().listFolder(this.createPath(rootDir)).execute();
			final Optional<RemoteEntry> blob = remoteFolder.children().stream().filter(re -> re.name().equals(name))
					.findFirst();
			return blob.isPresent();
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
	public SortedSet<String> getBlobKeysInsideContainer(String container, String prefix) throws IOException {
		try {
			final RemoteFolder remoteFolder = this.getApiClient().listFolder(createPath(container), true).execute();
			final SortedSet<String> keys = getBlobKeysInsideContainer(remoteFolder, prefix);
			return keys;
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public Blob getBlob(String containerName, String blobName) {
		try {
			final RemoteFile remoteFile = this.getApiClient().loadFile(createPath(containerName, blobName)).execute();

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
			blob.getMetadata().setContainer(containerName);
			return blob;
		} catch (ApiError e) {
			final PCloudError pCloudError = PCloudError.parse(e);
			if (pCloudError == PCloudError.FILE_NOT_FOUND || pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND) {
				logger.debug("Blob {}/{} not found: {}", containerName, blobName, pCloudError.getCode());
				return null;
			}
			if (pCloudError == PCloudError.PARENT_DIR_DOES_NOT_EXISTS) {
				logger.warn("Parent folder of blob {}/{} does not exist", containerName, blobName);
				return null;
			}
			throw new PCloudBlobStoreException(e);
		} catch (IOException e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public String putBlob(String containerName, Blob blob) throws IOException {
		this.pCloudContainerNameValidator.validate(containerName);
		this.pCloudBlobKeyValidator.validate(blob.getMetadata().getName());

		if (getDirectoryBlobSuffix(blob.getMetadata().getName()) != null) {
			return putDirectoryBlob(containerName, blob);
		}

		final String name = getFileOfKey(blob.getMetadata().getName());

		final String rootDir = getFolderOfKey(containerName, blob.getMetadata().getName());

		try {
			// Check if parent folder exists
			final RemoteFolder parentFolder = checkAndCreateParentFolder(rootDir);
			final RemoteFile remoteFile = this.getApiClient()
					.createFile(parentFolder, name, dataSourceFromBlob(blob), UploadOptions.OVERRIDE_FILE).execute();
			return remoteFile.hash();
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public void removeBlob(String container, String key) {
		this.pCloudContainerNameValidator.validate(container);
		this.pCloudBlobKeyValidator.validate(key);
		final String path = createPath(container, key);
		try {
			Boolean result = this.getApiClient().deleteFile(path).execute();
			if (!result) {
				logger.error("Failed to delete %s", path);
			}
		} catch (IOException | ApiError e) {
			logger.error("Failed to delete %s: %s", path, e);
			throw new PCloudBlobStoreException(e);
		}

	}

	@Override
	public BlobAccess getBlobAccess(String container, String key) {
		this.logger.debug("Retrieving access rights for PCloud blobs not supported: %s%s%s", container, getSeparator(),
				key);
		return BlobAccess.PRIVATE;
	}

	@Override
	public void setBlobAccess(String container, String key, BlobAccess access) {
		this.logger.debug("Setting access rights for PCloud blobs not supported: %s%s%s", container, getSeparator(),
				key);
	}

	@Override
	public Location getLocation(String containerName) {
		return defaultLocation.get();
	}

	@Override
	public String getSeparator() {
		return SEPARATOR;
	}

	/**
	 * Creates a directory within a container
	 * 
	 * @param containerName Name of the container
	 * @param directory     directory created
	 */
	public void createDirectory(String containerName, String directory) {
		this.pCloudContainerNameValidator.validate(containerName);
		this.pCloudBlobKeyValidator.validate(directory);
		try {
			this.getApiClient().createFolder(this.createPath(containerName, directory)).execute();
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	/**
	 * Counts the number of blobs within a container
	 * 
	 * @param container
	 * @param options
	 * @return
	 */
	public long countBlobs(String container, ListContainerOptions options) {
		try {
			return this.getApiClient().listFolder(createPath(container)).execute().children().size();
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	/**
	 * Deletes a directory within a container
	 * 
	 * @param container
	 * @param directory
	 */
	public void deleteDirectory(String container, String directory) {
		try {
			this.getApiClient().deleteFolder(createPath(container, directory)).execute();
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	/**
	 * Checks if a directory exits within a container.
	 * 
	 * @param containerName
	 * @param directory
	 * @return
	 */
	public boolean directoryExists(String containerName, String directory) {
		try {
			return this.getApiClient().listFolder(createPath(containerName, directory)).execute().isFolder();
		} catch (IOException | ApiError e) {
			if (e instanceof ApiError && PCloudError.parse((ApiError) e) == PCloudError.DIRECTORY_DOES_NOT_EXIST) {
				return false;
			}
			throw new PCloudBlobStoreException(e);
		}
	}

}
