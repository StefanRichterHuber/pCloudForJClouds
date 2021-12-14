package com.github.stefanrichterhuber.pCloudForjClouds.config;

import org.jclouds.blobstore.BlobRequestSigner;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.LocalBlobRequestSigner;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.config.BlobStoreObjectModule;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.MultipartUploadFactory;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.PCloudBlobStore;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.MultipartUploadFactoryImpl;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.PCloudApiClientProvider;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileOps;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal.PCloudFileOpsImpl;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudBlobKeyValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudContainerNameValidator;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.internal.PCloudBlobKeyValidatorImpl;
import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.internal.PCloudContainerNameValidatorImpl;
import com.google.inject.AbstractModule;
import com.pcloud.sdk.ApiClient;

public class PCloudCustomBlobStoreContextModule extends AbstractModule{
	@Override
	protected void configure() {
		bind(BlobStore.class).to(PCloudBlobStore.class);
		install(new BlobStoreObjectModule());
		bind(ConsistencyModel.class).toInstance(ConsistencyModel.STRICT);
		bind(ApiClient.class).toProvider(PCloudApiClientProvider.class);
		bind(PCloudBlobKeyValidator.class).to(PCloudBlobKeyValidatorImpl.class);
		bind(PCloudContainerNameValidator.class).to(PCloudContainerNameValidatorImpl.class);
		bind(PCloudFileOps.class).to(PCloudFileOpsImpl.class);
		bind(BlobRequestSigner.class).to(LocalBlobRequestSigner.class);
		bind(MultipartUploadFactory.class).to(MultipartUploadFactoryImpl.class);
	}
}
