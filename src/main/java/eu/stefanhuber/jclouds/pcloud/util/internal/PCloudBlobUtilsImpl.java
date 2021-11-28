package eu.stefanhuber.jclouds.pcloud.util.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.inject.Provider;

import org.jclouds.blobstore.LocalStorageStrategy;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.util.BlobUtils;

import com.google.inject.Inject;

import eu.stefanhuber.jclouds.pcloud.strategy.internal.PCloudStorageStrategyImpl;

public class PCloudBlobUtilsImpl implements BlobUtils {
	protected final PCloudStorageStrategyImpl storageStrategy;
	protected final Provider<BlobBuilder> blobBuilders;

	@Inject
	public PCloudBlobUtilsImpl(LocalStorageStrategy storageStrategy, Provider<BlobBuilder> blobBuilders) {
		this.storageStrategy = (PCloudStorageStrategyImpl) checkNotNull(storageStrategy,
				"PCloud Storage Strategy");
		this.blobBuilders = checkNotNull(blobBuilders, "Filesystem  blobBuilders");
	}

	@Override
	public BlobBuilder blobBuilder() {
		return blobBuilders.get();
	}

	@Override
	public boolean directoryExists(String containerName, String directory) {
		return storageStrategy.directoryExists(containerName, directory);
	}

	@Override
	public void createDirectory(String containerName, String directory) {
		storageStrategy.createDirectory(containerName, directory);
	}

	@Override
	public long countBlobs(String container, ListContainerOptions options) {
		return storageStrategy.countBlobs(container, options);
	}

	@Override
	public void clearContainer(String container, ListContainerOptions options) {
		storageStrategy.clearContainer(container, options);
	}

	@Override
	public void deleteDirectory(String container, String directory) {
		storageStrategy.deleteDirectory(container, directory);
	}
}
