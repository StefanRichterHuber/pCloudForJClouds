package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import static com.google.common.io.BaseEncoding.base16;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteArrayPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.BlobHashes;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.ExternalBlobMetadata;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.MetadataStrategy;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileDescriptor;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileDescriptor.LockType;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileLock;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileOps;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileOps.Flag;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.RemoteFile;

public class PCloudMultipartUploadImpl extends PCloudMultipartUpload {
    private static final int BUFFER_SIZE = 5 * 1024 * 1024; // 5MB
    private static final Logger LOGGER = LoggerFactory.getLogger(PCloudMultipartUploadImpl.class);
    private static final String MULTIPART_PREFIX = ".mpus-";
    private static final long WRITE_RESCHEDULE_DELAY_MS = 1000;
    private static final Executor WRITE_RESCHEDULE_EXECUTOR = CompletableFuture.delayedExecutor(
            WRITE_RESCHEDULE_DELAY_MS,
            TimeUnit.MILLISECONDS);
    private static final int WRITE_APPEND_RETRIES = 100;

    private final PCloudFileOps fileOps;
    private final ApiClient apiClient;
    private final MetadataStrategy metadataStrategy;

    private volatile AtomicLong currentPartId = new AtomicLong(0);

    private long temporaryFileId;
    private final String temporaryFileName;

    private final BlobHashes.Builder hashBuilder = new BlobHashes.Builder();

    /**
     * Creates a new {@link PCloudMultipartUpload}
     * 
     * @param folderId      Parent folder id containing the file to upload
     * @param containerName Container containing the file to upload
     * @param blobName      Name of the file
     * @param id            ID of the multipart upload
     * @param blobMetadata  Metadata of the target upload
     * @param putOptions    {@link PutOptions} of the upload
     * @return {@link PCloudMultipartUpload}
     */
    public PCloudMultipartUploadImpl(final ApiClient apiClient, final MetadataStrategy metadataStrategy,
            final PCloudFileOps fileOps,
            final long folderId, @Nonnull final String containerName,
            final String blobName, final String id, final BlobMetadata blobMetadata, final PutOptions putOptions) {
        super(containerName, folderId, blobName, id, blobMetadata, putOptions);
        this.fileOps = fileOps;
        this.apiClient = apiClient;
        this.metadataStrategy = metadataStrategy;
        this.temporaryFileName = MULTIPART_PREFIX + this.id() + "-" + blobName + "-stub";

    }

    @Override
    public CompletableFuture<Void> start() {
        LOGGER.debug("Initiated multipart upload to file {} at folder {} in container {} with upload id {}", blobName,
                folderId, containerName, id);
        // Create the empty dummy file and then do nothing
        try {
            final RemoteFile remoteFile = this.apiClient.createFile(folderId, temporaryFileName, DataSource.EMPTY)
                    .execute();
            this.temporaryFileId = remoteFile.fileId();
        } catch (IOException | ApiError e) {
            return CompletableFuture.failedFuture(e);
        }
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Creates a local copy of the given {@link Payload} copying its content to a
     * byte array
     * 
     * @param payload Payload to copy
     * @return Copied payload.
     * @throws IOException
     */
    private static Payload copy(final Payload payload) throws IOException {
        final byte[] content = new byte[payload.getContentMetadata().getContentLength().intValue()];
        try {
            final MessageDigest md5MD = MessageDigest.getInstance("MD5");
            try (InputStream is = new DigestInputStream(new BufferedInputStream(payload.openStream()), md5MD)) {
                IOUtils.read(is, content);
            }
            final ByteArrayPayload byteArrayPayload = new ByteArrayPayload(content, md5MD.digest());
            return byteArrayPayload;
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private CompletableFuture<MultipartPart> append(final int partNumber, final Payload payload, int retries) {
        LOGGER.debug("Received part {} for multipart upload {}", partNumber, id());
        if (partNumber == currentPartId.get() + 1) {
            MessageDigest md5MD;
            try {
                md5MD = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                return CompletableFuture.failedFuture(e);
            }
            // We need to additionally calculate the checksum of only the part to transmit
            try (InputStream src = new DigestInputStream(hashBuilder.wrap(payload.openStream()),
                    md5MD)) {

                byte[] buf = new byte[BUFFER_SIZE];
                int bytesRead = src.read(buf);
                while (bytesRead > 0) {
                    try (PCloudFileDescriptor fd = fileOps.open(temporaryFileId, Flag.APPEND);) {
                        try (OutputStream target = fd.openStream()) {
                            target.write(buf, 0, bytesRead);
                            LOGGER.info("Written chunk of {} bytes of part {} of multipart upload {}", bytesRead,
                                    partNumber, id());
                        }
                    }
                    bytesRead = src.read(buf);
                }
            } catch (final java.net.SocketTimeoutException e) {
                LOGGER.warn("Failed wo write Multipart part {} of upload {} due to SocketTimeOut", partNumber, id());
                return retryAppend(partNumber, payload, retries - 1);
            } catch (final IOException e) {
                LOGGER.warn("Failed to write Multipart part.", e);
                return retryAppend(partNumber, payload, retries - 1);
            }
            final byte[] md5 = md5MD.digest();
            currentPartId.incrementAndGet();
            final MultipartPart multipartPart = MultipartPart.create(partNumber,
                    payload.getContentMetadata().getContentLength(),
                    base16().lowerCase().encode(md5),
                    null);
            this.parts.add(multipartPart);
            return CompletableFuture.completedFuture(multipartPart);
        } else {
            LOGGER.info("Failed to upload part {} for multipart upload {}. Received out-of-order", partNumber, id());
            return retryAppend(partNumber, payload, retries - 1);
        }
    }

    private CompletableFuture<MultipartPart> retryAppend(final int partNumber, final Payload payload, int retries) {
        if (retries > 0) {
            LOGGER.info(
                    "Received part {} for multipart upload {} is out-of-order or failed. Retry in {} ms. {} retries left",
                    partNumber, id(),
                    WRITE_RESCHEDULE_DELAY_MS, retries);

            return CompletableFuture.supplyAsync(() -> null, WRITE_RESCHEDULE_EXECUTOR)
                    .thenCompose(b -> append(partNumber, payload, retries));

        } else {
            LOGGER.error("Failed to upload part {} of multipart upload {}", partNumber, id());
            return CompletableFuture.failedFuture(
                    new RuntimeException("Failed to upload part " + partNumber + " of multipart upload " + id()));
        }
    }

    @Override
    public CompletableFuture<MultipartPart> append(final int partNumber, final Payload payload) {
        return this.append(partNumber, payload, WRITE_APPEND_RETRIES);
    }

    @Override
    public CompletableFuture<Void> abort() {
        /*
         * We need exclusive access for the file, and then delete it
         */
        try (PCloudFileDescriptor fd = fileOps.open(folderId, blobName, Flag.APPEND);) {
            try (PCloudFileLock lock = fd.lock(LockType.EXCLUSIVE_LOCK, false)) {
                if (lock != null) {
                    this.apiClient.deleteFile(temporaryFileId);
                }
            }

        } catch (final IOException e) {
            return CompletableFuture.failedFuture(e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<String> complete() {

        // Lock to ensure no ongoing op happens
        // First ensure the queue is written
        LOGGER.debug("Completed upload of all parts", id());

        // Then rename the target file to its final file name.
        return PCloudUtils.execute(this.apiClient.renameFile(temporaryFileId, blobName)) //
                .thenCompose(remoteFile -> {
                    LOGGER.info("Renamed multipart temporary file {} to {} within folder {} (=> {})", temporaryFileName,
                            blobName, folderId(), this.blobMetadata.getName());
                    LOGGER.debug("Received the final checksum from the backend: {}", remoteFile.hash());
                    // Upload metadata
                    // Warning: Sometimes the build-in hash of the remotefile changes after some
                    // seconds!!
                    final BlobHashes hashes = this.hashBuilder.toBlobHashes(remoteFile.hash());
                    final ExternalBlobMetadata externalBlobMetadata = new ExternalBlobMetadata(
                            this.containerName(),
                            this.blobMetadata.getName(),
                            remoteFile.fileId(),
                            folderId,
                            StorageType.BLOB,
                            BlobAccess.PRIVATE,
                            hashes,
                            this.blobMetadata.getUserMetadata());
                    return this.metadataStrategy.put(containerName, this.blobMetadata.getName(), externalBlobMetadata)
                            .thenApply(v -> hashes.md5());
                });

    }

    @Override
    public MultipartUpload getUpload() {
        return this;
    }

}
