package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.logging.Logger;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.MetadataStrategy;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.MultipartUploadFactory;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileOps;
import com.pcloud.sdk.ApiClient;

public class MultipartUploadFactoryImpl implements MultipartUploadFactory {
    @Resource
    protected Logger logger = Logger.NULL;

    private final ApiClient apiClient;
    private final PCloudFileOps fileOps;
    private final MetadataStrategy metadataStrategy;
    private final BlobStore blobStore;

    @Inject
    protected MultipartUploadFactoryImpl(ApiClient apiClient, PCloudFileOps fileOps,
            MetadataStrategy metadataStrategy, BlobStore blobStore) {
        this.apiClient = checkNotNull(apiClient, "PCloud api client");
        this.fileOps = checkNotNull(fileOps, "PCloud File ops client");
        this.metadataStrategy = checkNotNull(metadataStrategy, "Metadatastrategy");
        this.blobStore = blobStore;
    }

    @Override
    public MultipartUploadLifecyle create(long folderId, String containerName, String blobName, String id,
            BlobMetadata blobMetadata, PutOptions putOptions) {

        return new TemporaryFileMultipartUploadImpl(containerName, blobName, id, blobMetadata, putOptions, blobStore);

        // return new PCloudMultipartUploadImpl(this.apiClient, this.metadataStrategy,
        // this.fileOps, folderId,
        // containerName, blobName, id,
        // blobMetadata, putOptions);
    }

}
