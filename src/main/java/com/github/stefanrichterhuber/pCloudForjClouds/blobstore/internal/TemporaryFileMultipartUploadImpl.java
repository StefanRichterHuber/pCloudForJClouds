package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import static com.google.common.io.BaseEncoding.base16;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;

import javax.annotation.Nonnull;

import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.BlobHashes;

public class TemporaryFileMultipartUploadImpl extends MultipartUpload implements MultipartUploadLifecyle {
    private static final Logger LOGGER = LoggerFactory.getLogger(TemporaryFileMultipartUploadImpl.class);

    protected record MultipartFiles(File file, long size, MultipartPart multipartPart)
            implements Comparable<MultipartFiles> {

        @Override
        public int compareTo(MultipartFiles o) {
            return Long.compare(this.multipartPart.partNumber(), o.multipartPart.partNumber());
        }
    }

    @Nonnull
    private final String containerName;
    private final String blobName;
    private final String id;
    private final BlobMetadata blobMetadata;
    private final PutOptions putOptions;
    private final BlobStore blobStore;

    private final BlobHashes.Builder hashBuilder = new BlobHashes.Builder();

    protected Set<MultipartFiles> parts = new ConcurrentSkipListSet<>();

    public TemporaryFileMultipartUploadImpl(@Nonnull String containerName, String blobName, String id,
            BlobMetadata blobMetadata,
            PutOptions putOptions, BlobStore blobStore) {
        super();
        this.containerName = containerName;
        this.blobName = blobName;
        this.id = id;
        this.blobMetadata = blobMetadata;
        this.putOptions = putOptions;
        this.blobStore = blobStore;
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

    @Override
    public CompletableFuture<Void> start() {
        LOGGER.debug("Initiated multipart upload to file {} in container {} with upload id {}", blobName,
                containerName, id);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<MultipartPart> append(final int partNumber, final Payload payload) {
        try {
            File tempFile = Files.createTempFile(null, null).toFile();
            LOGGER.info("Received part {} for multipart upload {}", partNumber, id());

            MessageDigest md5MD;
            try {
                md5MD = MessageDigest.getInstance("MD5");
            } catch (NoSuchAlgorithmException e) {
                return CompletableFuture.failedFuture(e);
            }
            long size = -1;
            try (FileOutputStream bos = new FileOutputStream(tempFile);
                    InputStream src = new DigestInputStream(hashBuilder.wrap(payload.openStream()),
                            md5MD)) {
                size = IOUtils.copyLarge(src, bos);
            }
            byte[] md5 = md5MD.digest();
            String md5Hex = base16().lowerCase().encode(md5);

            LOGGER.info("Written part {} for multipart upload {} to file {} with size {} bytes and MD5 {} ", partNumber,
                    id(), tempFile,
                    size, md5Hex);

            final MultipartPart multipartPart = MultipartPart.create(partNumber,
                    payload.getContentMetadata().getContentLength(),
                    md5Hex, null);
            this.parts.add(new MultipartFiles(tempFile, size, multipartPart));
            return CompletableFuture.completedFuture(multipartPart);
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> abort() {
        for (MultipartFiles part : parts) {
            part.file().delete();
        }

        return CompletableFuture.completedFuture(null);

    }

    @Override
    public CompletableFuture<String> complete() {

        final MultiFilePayload payload = new MultiFilePayload(parts.stream().map(MultipartFiles::file).toList());
        final long contentLength = parts.stream().mapToLong(MultipartFiles::size).sum();
        payload.getContentMetadata().setContentLength(contentLength);
        payload.getContentMetadata().setContentType(this.blobMetadata.getContentMetadata().getContentType());

        final String etag = blobStore.putBlob(containerName,
                blobStore.blobBuilder(this.blobMetadata.getName()) //
                        .payload(payload) //
                        .contentType(this.blobMetadata.getContentMetadata().getContentType()) //
                        .userMetadata(this.blobMetadata.getUserMetadata()) //
                        .build(),
                putOptions);

        payload.deleteFiles();

        return CompletableFuture.completedFuture(etag);
    }

    @Override
    public List<MultipartPart> getParts() {
        return parts.stream().map(MultipartFiles::multipartPart).toList();
    }

    @Override
    public MultipartUpload getUpload() {
        return this;
    }

}
