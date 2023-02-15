package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.Nonnull;

import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.Payload;

import com.pcloud.sdk.RemoteFile;

/**
 * Base class for {@link MultipartUpload}s to pCloud
 * 
 * @author Stefan Richter-Huber
 *
 */
public abstract class PCloudMultipartUpload extends MultipartUpload {
    @Nonnull
    protected final String containerName;
    protected final String blobName;
    protected final String id;
    protected final BlobMetadata blobMetadata;
    protected final PutOptions putOptions;
    protected final long folderId;

    protected List<MultipartPart> parts = new CopyOnWriteArrayList<>();

    public PCloudMultipartUpload(@Nonnull String containerName, long folderId, String blobName, String id,
            BlobMetadata blobMetadata,
            PutOptions putOptions) {
        super();
        this.containerName = containerName;
        this.blobName = blobName;
        this.id = id;
        this.blobMetadata = blobMetadata;
        this.putOptions = putOptions;
        this.folderId = folderId;
    }

    /**
     * Starts the upload of the file.
     */
    public abstract void start();

    /**
     * Append new part to this upload.
     * 
     * @param partNumber
     * @param payload
     * @return
     */
    public abstract CompletableFuture<MultipartPart> append(int partNumber, Payload payload) throws IOException;

    /**
     * Stops the upload of the file and deletes it Rethrows all exceptions happened
     * in the other thread
     */
    public abstract void abort();

    /**
     * Completes the multipart upload and returns the generated {@link RemoteFile}.
     * 
     * @return ETag of the generated file
     */
    public abstract CompletableFuture<String> complete();

    /**
     * Folder id of the parent folder to create blob in
     * 
     * @return
     */
    public long folderId() {
        return this.folderId;
    }

    @Override
    public String containerName() {
        return containerName;
    }

    @Override
    public String blobName() {
        return blobName;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public BlobMetadata blobMetadata() {
        return blobMetadata;
    }

    @Override
    public PutOptions putOptions() {
        return putOptions;
    }

    /**
     * Currently registerd parts of this multipart upload
     * 
     * @return
     */
    public List<MultipartPart> getParts() {
        return parts;
    }

    public static final class QueueEntry implements Comparable<QueueEntry> {
        private final int partNumber;
        private final Payload payload;

        @Override
        public int compareTo(QueueEntry o) {
            return Integer.compare(partNumber, o.partNumber);
        }

        public QueueEntry(int partNumber, Payload payload) {
            super();
            this.partNumber = partNumber;
            this.payload = payload;
        }

        public int getPartNumber() {
            return partNumber;
        }

        public Payload getPayload() {
            return payload;
        }
    }

    @Override
    public String toString() {
        return "PCloudMultipartUpload with id " + id + " [containerName=" + containerName + ", blobName=" + blobName
                + ", folderId=" + folderId + "]";
    }
}
