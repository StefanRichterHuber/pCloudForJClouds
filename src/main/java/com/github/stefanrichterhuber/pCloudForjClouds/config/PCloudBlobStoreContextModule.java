package com.github.stefanrichterhuber.pCloudForjClouds.config;

import org.jclouds.blobstore.BlobRequestSigner;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.LocalBlobRequestSigner;
import org.jclouds.blobstore.LocalStorageStrategy;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.config.BlobStoreObjectModule;
import org.jclouds.blobstore.config.LocalBlobStore;
import org.jclouds.blobstore.util.BlobUtils;

import com.github.stefanrichterhuber.pCloudForjClouds.connection.PCloudApiClientProvider;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudBlobKeyValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudContainerNameValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.internal.PCloudBlobKeyValidatorImpl;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.internal.PCloudContainerNameValidatorImpl;
import com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal.PCloudStorageStrategyImpl;
import com.github.stefanrichterhuber.pCloudForjClouds.util.internal.PCloudBlobUtilsImpl;
import com.google.inject.AbstractModule;
import com.pcloud.sdk.ApiClient;

public class PCloudBlobStoreContextModule extends AbstractModule {

	@Override
	protected void configure() {
		bind(BlobStore.class).to(LocalBlobStore.class);
		install(new BlobStoreObjectModule());
		bind(ConsistencyModel.class).toInstance(ConsistencyModel.STRICT);
		bind(ApiClient.class).toProvider(PCloudApiClientProvider.class);
		bind(LocalStorageStrategy.class).to(PCloudStorageStrategyImpl.class);
		bind(BlobUtils.class).to(PCloudBlobUtilsImpl.class);
		bind(PCloudBlobKeyValidator.class).to(PCloudBlobKeyValidatorImpl.class);
		bind(PCloudContainerNameValidator.class).to(PCloudContainerNameValidatorImpl.class);
		bind(BlobRequestSigner.class).to(LocalBlobRequestSigner.class);
	}

}
