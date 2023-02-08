package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import static com.google.common.io.BaseEncoding.base16;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MultipartPart;
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
import com.google.common.hash.Hashing;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.RemoteFile;

public class PCloudMultipartUploadImpl extends PCloudMultipartUpload {
    private static final int BUFFER_SIZE = 1 * 1024 * 1024; // 1MB
    private static final Logger LOGGER = LoggerFactory.getLogger(PCloudMultipartUploadImpl.class);
    private static final String MULTIPART_PREFIX = ".mpus-";

    private static final byte[] EMPTY_CONTENT = new byte[0];
    @SuppressWarnings("deprecation")
    private static final byte[] EMPTY_CONTENT_MD5 = Hashing.md5().hashBytes(EMPTY_CONTENT).asBytes();
    private static final String PART_ETAG = base16().lowerCase().encode(EMPTY_CONTENT_MD5);

    private final PCloudFileOps fileOps;
    private final ApiClient apiClient;
    private final MetadataStrategy metadataStrategy;

    private volatile long currentPartId = 0l;

    private final PriorityBlockingQueue<QueueEntry> queue = new PriorityBlockingQueue<>();

    private long temporaryFileId;
    private final String temporaryFileName;

    private final BlobHashes.Builder hashBuilder = new BlobHashes.Builder();

    private final Lock writeLock = new ReentrantLock();

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
    public PCloudMultipartUploadImpl(ApiClient apiClient, MetadataStrategy metadataStrategy, PCloudFileOps fileOps,
            long folderId, String containerName,
            String blobName, String id, BlobMetadata blobMetadata, PutOptions putOptions) {
        super(containerName, folderId, blobName, id, blobMetadata, putOptions);
        this.fileOps = fileOps;
        this.apiClient = apiClient;
        this.metadataStrategy = metadataStrategy;
        this.temporaryFileName = MULTIPART_PREFIX + this.id() + "-" + blobName + "-stub";

    }

    @Override
    public void start() {
        LOGGER.debug("Initiated multipart upload to file {} at folder {} in container {} with upload id {}", blobName,
                folderId, containerName, id);
        // Create the empty dummy file and then do nothing
        try {
            final RemoteFile remoteFile = this.apiClient.createFile(folderId, temporaryFileName, DataSource.EMPTY)
                    .execute();
            this.temporaryFileId = remoteFile.fileId();
        } catch (IOException | ApiError e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a local copy of the given {@link Payload} copying its content to a
     * byte array
     * 
     * @param payload Payload to copy
     * @return Copied payload.
     * @throws IOException
     */
    private static Payload copy(Payload payload) throws IOException {
        final byte[] content = new byte[payload.getContentMetadata().getContentLength().intValue()];
        try {
            MessageDigest md5MD = MessageDigest.getInstance("MD5");
            try (InputStream is = new DigestInputStream(new BufferedInputStream(payload.openStream()), md5MD)) {
                IOUtils.read(is, content);
            }
            final ByteArrayPayload byteArrayPayload = new ByteArrayPayload(content, md5MD.digest());
            return byteArrayPayload;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public CompletableFuture<MultipartPart> append(int partNumber, Payload payload) throws IOException {
        return CompletableFuture.supplyAsync(() -> {
            try {
                LOGGER.debug("Received part {} for multipart upload {}", partNumber, id());
                // First we try to directly write to the file
                boolean written = false;
                byte[] md5 = null;
                if (writeLock.tryLock()) {
                    try (PCloudFileDescriptor fd = fileOps.open(temporaryFileId, Flag.APPEND);
                            OutputStream target = fd.openStream();
                            BufferedOutputStream bos = new BufferedOutputStream(target, BUFFER_SIZE)) {
                        // Are there any previous parts of the queue to write?
                        this.writeQueue(bos);
                        // Then check if this part is next in the queue
                        if (partNumber == currentPartId + 1) {
                            currentPartId++;
                            MessageDigest md5MD = MessageDigest.getInstance("MD5");
                            // We need to additionally calculate the checksum of only the part to transmit
                            try (InputStream src = new DigestInputStream(hashBuilder.wrap(payload.openStream()),
                                    md5MD)) {
                                IOUtils.copyLarge(src, bos);
                            }
                            md5 = md5MD.digest();
                            written = true;
                            LOGGER.debug("Directly uploaded part {} for multipart upload {}", partNumber, id());
                        }
                        // Are there any next parts waiting in the queue to be written?
                        this.writeQueue(bos);
                    } catch (NoSuchAlgorithmException e) {
                        throw new RuntimeException(e);
                    } finally {
                        writeLock.unlock();
                    }
                }
                if (!written) {
                    LOGGER.debug("Queueing received part {} for multipart upload {}", partNumber, id());
                    // We were unable to directly write the payload -> create copy of the payload
                    // and add it to the queue
                    final Payload localPayload = copy(payload);
                    md5 = localPayload.getContentMetadata().getContentMD5AsHashCode().asBytes();
                    queue.add(new QueueEntry(partNumber, localPayload));
                }

                final MultipartPart multipartPart = MultipartPart.create(partNumber,
                        payload.getContentMetadata().getContentLength(),
                        md5 != null ? base16().lowerCase().encode(md5) : PART_ETAG, null);
                this.parts.add(multipartPart);
                return multipartPart;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Writes all suitable entries of the queue to the given {@link OutputStream}.
     * 
     * @param target
     * @throws IOException
     */
    private void writeQueue(OutputStream target) throws IOException {
        while (!queue.isEmpty() && queue.peek().getPartNumber() == currentPartId + 1) {
            QueueEntry next = queue.poll();
            if (next != null && next.getPartNumber() == currentPartId + 1) {
                currentPartId++;
                try (InputStream src = hashBuilder.wrap(next.getPayload().openStream())) {
                    IOUtils.copyLarge(src, target);
                }
                LOGGER.debug("Uploaded queued part {} for multipart upload {}", next.getPartNumber(), id());
            } else {
                // Add back to queue
                queue.add(next);
            }
        }
    }

    /**
     * Acquires the lock and tries to write all pending parts to the output stream.
     * 
     * @throws IOException
     */
    private void writeQueue() throws IOException {
        if (writeLock.tryLock()) {
            if (!queue.isEmpty()) {
                // check if we get exclusive access to the file
                try (PCloudFileDescriptor fd = fileOps.open(temporaryFileId, Flag.APPEND);) {
                    OutputStream target = new BufferedOutputStream(fd.openStream(), BUFFER_SIZE);
                    writeQueue(target);
                } finally {
                    writeLock.unlock();
                }
            }
        }
    }

    @Override
    public void abort() {
        /*
         * We need exclusive access for the file, and then delete it
         */
        try (PCloudFileDescriptor fd = fileOps.open(folderId, blobName, Flag.APPEND);) {
            try (PCloudFileLock lock = fd.lock(LockType.EXCLUSIVE_LOCK, false)) {
                if (lock != null) {
                    this.apiClient.deleteFile(temporaryFileId);
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public CompletableFuture<String> complete() {
        // First ensure the queue is written
        try {
            writeQueue();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (!this.queue.isEmpty()) {
            throw new IllegalStateException("There are parts missing to complete the upload");
        }
        LOGGER.debug("Completed upload of all parts", id());

        // Then rename the target file to its final file name.
        return PCloudUtils.execute(this.apiClient.renameFile(temporaryFileId, blobName)) //
                .thenCompose(remoteFile -> {
                    LOGGER.debug("Renamed multipart temporary file {} to {}", temporaryFileName, blobName);
                    LOGGER.debug("Received the final checksum from the backend: {}", remoteFile.hash());
                    // Upload metadata
                    // Warning: Sometimes the build-in hash of the remotefile changes after some
                    // seconds!!
                    final BlobHashes hashes = this.hashBuilder.toBlobHashes(remoteFile.hash());
                    final ExternalBlobMetadata externalBlobMetadata = new ExternalBlobMetadata(
                            this.containerName(),
                            this.blobName(),
                            remoteFile.fileId(),
                            StorageType.BLOB,
                            BlobAccess.PRIVATE,
                            hashes,
                            this.blobMetadata.getUserMetadata());
                    return this.metadataStrategy.put(containerName, blobName, externalBlobMetadata)
                            .thenApply(v -> hashes.md5());
                });

    }

}
