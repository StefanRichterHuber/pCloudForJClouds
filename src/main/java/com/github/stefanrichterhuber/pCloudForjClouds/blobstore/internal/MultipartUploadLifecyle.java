package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.io.Payload;

import com.pcloud.sdk.RemoteFile;

public interface MultipartUploadLifecyle {
    /**
     * Starts the upload of the file.
     */
    CompletableFuture<Void> start();

    /**
     * Append new part to this upload.
     * 
     * @param partNumber
     * @param payload
     * @return
     */
    CompletableFuture<MultipartPart> append(int partNumber, Payload payload);

    /**
     * Stops the upload of the file and deletes it Rethrows all exceptions happened
     * in the other thread
     */
    CompletableFuture<Void> abort();

    /**
     * Completes the multipart upload and returns the generated {@link RemoteFile}.
     * 
     * @return ETag of the generated file
     */
    CompletableFuture<String> complete();

    /**
     * Currently registered parts of this multipart upload
     * 
     * @return
     */
    List<MultipartPart> getParts();

    /**
     * Get the {@link MultipartUpload} associated with this lifecycle.
     * 
     * @return
     */
    MultipartUpload getUpload();

}
