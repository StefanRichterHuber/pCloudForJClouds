package eu.stefanhuber.jclouds.pcloud.config;

import org.jclouds.blobstore.BlobRequestSigner;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.LocalBlobRequestSigner;
import org.jclouds.blobstore.LocalStorageStrategy;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.config.BlobStoreObjectModule;
import org.jclouds.blobstore.config.LocalBlobStore;
import org.jclouds.blobstore.util.BlobUtils;

import com.google.inject.AbstractModule;

import eu.stefanhuber.jclouds.pcloud.predicates.validators.PCloudBlobKeyValidator;
import eu.stefanhuber.jclouds.pcloud.predicates.validators.PCloudContainerNameValidator;
import eu.stefanhuber.jclouds.pcloud.predicates.validators.internal.PCloudBlobKeyValidatorImpl;
import eu.stefanhuber.jclouds.pcloud.predicates.validators.internal.PCloudContainerNameValidatorImpl;
import eu.stefanhuber.jclouds.pcloud.strategy.internal.PCloudStorageStrategyImpl;
import eu.stefanhuber.jclouds.pcloud.util.internal.PCloudBlobUtilsImpl;

public class PCloudBlobStoreContextModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(BlobStore.class).to(LocalBlobStore.class);
		install(new BlobStoreObjectModule());
		bind(ConsistencyModel.class).toInstance(ConsistencyModel.STRICT);
		bind(LocalStorageStrategy.class).to(PCloudStorageStrategyImpl.class);
		bind(BlobUtils.class).to(PCloudBlobUtilsImpl.class);
		bind(PCloudBlobKeyValidator.class).to(PCloudBlobKeyValidatorImpl.class);
		bind(PCloudContainerNameValidator.class).to(PCloudContainerNameValidatorImpl.class);
		bind(BlobRequestSigner.class).to(LocalBlobRequestSigner.class);
	}

}
