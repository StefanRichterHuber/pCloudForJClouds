package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.options.PutOptions;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudMultipartUpload;

/**
 * Describes a factory for {@link PCloudMultipartUpload}s.
 * @author Stefan Richter-Huber
 *
 */
public interface MultipartUploadFactory {
	/**
	 * Creates a new {@link PCloudMultipartUpload}
	 * @param folderId Parent folder id containing the file to upload
	 * @param containerName Container containg the file to upload
	 * @param blobName Name of the file 
	 * @param id ID of the multipart upload
	 * @param blobMetadata Metadata of the target upload
	 * @param putOptions {@link PutOptions} of the upload
	 * @return {@link PCloudMultipartUpload}
	 */
	PCloudMultipartUpload create(long folderId, String containerName, String blobName, String id,
			BlobMetadata blobMetadata, PutOptions putOptions) ;
}
