package eu.stefanhuber.jclouds.pcloud.strategy.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Provider;

import org.apache.commons.io.IOUtils;
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
import org.jclouds.domain.Location;
import org.jclouds.logging.Logger;

import com.google.common.base.Supplier;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.Authenticators;
import com.pcloud.sdk.Call;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.PCloudSdk;
import com.pcloud.sdk.RemoteEntry;
import com.pcloud.sdk.RemoteFile;
import com.pcloud.sdk.RemoteFolder;
import com.pcloud.sdk.UploadOptions;

import eu.stefanhuber.jclouds.pcloud.predicates.validators.PCloudBlobKeyValidator;
import eu.stefanhuber.jclouds.pcloud.predicates.validators.PCloudContainerNameValidator;
import eu.stefanhuber.jclouds.pcloud.reference.PCloudConstants;

public class PCloudStorageStrategyImpl implements LocalStorageStrategy {
	@Resource
	protected Logger logger = Logger.NULL;

	private static final String SEPARATOR = "/";

	protected final Provider<BlobBuilder> blobBuilders;
	protected final String baseDirectory;
	protected final PCloudContainerNameValidator pCloudContainerNameValidator;
	protected final PCloudBlobKeyValidator pCloudBlobKeyValidator;
	private final Supplier<Location> defaultLocation;
	private final ApiClient apiClient;

	@Inject
	protected PCloudStorageStrategyImpl(Provider<BlobBuilder> blobBuilders,
			@Named(PCloudConstants.PROPERTY_BASEDIR) String baseDir,
			@Named(PCloudConstants.PROPERTY_CLIENT_SECRET) String clientSecret,
			PCloudContainerNameValidator pCloudContainerNameValidator, PCloudBlobKeyValidator pCloudBlobKeyValidator,
			Supplier<Location> defaultLocation) {
		this.blobBuilders = checkNotNull(blobBuilders, "PCloud storage strategy blobBuilders");
		this.baseDirectory = checkNotNull(baseDir, "PCloud storage base directory");
		this.pCloudContainerNameValidator = checkNotNull(pCloudContainerNameValidator,
				"PCloud container name validator");
		this.pCloudBlobKeyValidator = checkNotNull(pCloudBlobKeyValidator, "PCloud blob key validator");
		this.defaultLocation = defaultLocation;
		this.apiClient = PCloudSdk.newClientBuilder()
				.authenticator(Authenticators.newOAuthAuthenticator(checkNotNull(clientSecret, "PCloud client secret")))
				.create();
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
		byte[] content = IOUtils.toByteArray(blob.getPayload().openStream());
		return DataSource.create(content);
	}

	@Override
	public boolean containerExists(String container) {
		this.pCloudContainerNameValidator.validate(container);
		try {
			final String path = createPath(container);
			RemoteFolder remoteFolder = this.getApiClient().listFolder(path).execute();
			return remoteFolder.isFolder();
		} catch (IOException | ApiError e) {
			if (e instanceof ApiError && PCloudError.parse((ApiError) e) == PCloudError.DIRECTORY_DOES_NOT_EXIST) {
				return false;
			}
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public Collection<String> getAllContainerNames() {
		try {
			final RemoteFolder rootFolder = this.getApiClient().listFolder(this.baseDirectory).execute();
			final List<String> result = rootFolder.children().stream().map(re -> re.name())
					.collect(Collectors.toList());
			return result;
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public boolean createContainerInLocation(String container, Location location, CreateContainerOptions options) {
		this.pCloudContainerNameValidator.validate(container);
		return this.createContainerInLocation(this.getLocation(container), container, options);
	}

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

	public boolean createContainerInLocation(Location location, String container, CreateContainerOptions options) {
		this.pCloudContainerNameValidator.validate(container);
		return createContainerInLocation(location, container);
	}

	@Override
	public ContainerAccess getContainerAccess(String container) {
		this.pCloudContainerNameValidator.validate(container);
		this.logger.warn("Retrieving access rights for PCloud containers not supported: %s", container);
		return ContainerAccess.PRIVATE;
	}

	@Override
	public void setContainerAccess(String container, ContainerAccess access) {
		this.pCloudContainerNameValidator.validate(container);
		this.logger.warn("Settings access rights for PCloud containers not supported: %s", container);
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
		this.pCloudContainerNameValidator.validate(container);
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
		} catch (IOException | ApiError e) {
			if (e instanceof ApiError && PCloudError.parse((ApiError) e) == PCloudError.DIRECTORY_DOES_NOT_EXIST) {
				return null;
			}
			throw new PCloudBlobStoreException(e);
		}

	}

	@Override
	public boolean blobExists(String container, String key) {
		this.pCloudContainerNameValidator.validate(container);
		this.pCloudBlobKeyValidator.validate(key);
		try {
			RemoteFolder remoteFolder = this.getApiClient().listFolder(this.createPath(container)).execute();
			Optional<RemoteEntry> blob = remoteFolder.children().stream().filter(RemoteEntry::isFile)
					.filter(re -> re.name().equals(key)).findFirst();
			return blob.isPresent();
		} catch (IOException | ApiError e) {
			if (e instanceof ApiError && PCloudError.parse((ApiError) e) == PCloudError.DIRECTORY_DOES_NOT_EXIST) {
				return false;
			}
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public Iterable<String> getBlobKeysInsideContainer(String container, String prefix) throws IOException {
		try {
			final RemoteFolder remoteFolder = this.getApiClient().listFolder(createPath(container)).execute();
			final List<String> keys = remoteFolder.children().stream().map(RemoteEntry::name)
					.filter(re -> prefix != null ? re.startsWith(prefix) : true).collect(Collectors.toList());
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

			final Blob blob = this.blobBuilders.get() //
					.payload(new RemoteFilePayload(remoteFile)) //
					.contentType(remoteFile.contentType()) //
					.contentLength(remoteFile.size()) //
					.eTag(remoteFile.hash()) //
					.name(remoteFile.name()) //
					.userMetadata(userMetadata) //
					.build();
			blob.getMetadata().setLastModified(remoteFile.lastModified());
			blob.getMetadata().setCreationDate(remoteFile.created());
			blob.getMetadata().setId(remoteFile.id());
			blob.getMetadata().setContainer(containerName);

			return blob;
		} catch (IOException | ApiError e) {
			throw new PCloudBlobStoreException(e);
		}
	}

	@Override
	public String putBlob(String containerName, Blob blob) throws IOException {
		this.pCloudContainerNameValidator.validate(containerName);
		try {
			final String name = blob.getMetadata().getName();
			final String path = createPath(containerName);
			final DataSource ds = dataSourceFromBlob(blob);
			final Call<RemoteFile> createdFile = this.getApiClient().createFile(path, name, ds,
					UploadOptions.OVERRIDE_FILE);
			RemoteFile remoteFile = createdFile.execute();
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
		this.logger.warn("Retrieving access rights for PCloud blobs not supported: %s%s%s", container, getSeparator(),
				key);
		return BlobAccess.PRIVATE;
	}

	@Override
	public void setBlobAccess(String container, String key, BlobAccess access) {
		this.logger.warn("Setting access rights for PCloud blobs not supported: %s%s%s", container, getSeparator(),
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
