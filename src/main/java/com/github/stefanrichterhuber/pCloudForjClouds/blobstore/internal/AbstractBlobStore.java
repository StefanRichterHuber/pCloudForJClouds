package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobMetadata;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.blobstore.options.PutOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Some provider independent implementations of {@link BlobStore}.
 * 
 * @author Stefan Richter-Huber
 *
 */
public abstract class AbstractBlobStore implements BlobStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBlobStore.class);

    @Override
    public void downloadBlob(String container, String name, File destination) {
        LOGGER.info("Download blob {}/{} to file destination {}", container, name, destination);

        try (InputStream ips = this.streamBlob(container, name); OutputStream fos = new FileOutputStream(destination)) {
            IOUtils.copy(ips, fos);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void downloadBlob(String container, String name, File destination, ExecutorService executor) {
        this.downloadBlob(container, name, destination);
    }

    @Override
    public InputStream streamBlob(String container, String name) {
        LOGGER.info("Stream blob {}/{}", container, name);

        final Blob blob = this.getBlob(container, name);
        if (blob != null && blob.getPayload() != null) {
            try {
                return blob.getPayload().openStream();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    @Override
    public InputStream streamBlob(String container, String name, ExecutorService executor) {
        return this.streamBlob(container, name);
    }

    @Override
    public long countBlobs(String container) {
        return countBlobs(container, ListContainerOptions.Builder.recursive());
    }

    @Override
    public long countBlobs(String container, ListContainerOptions options) {
        LOGGER.info("Count blobs in container {} with options", container, options);
        final PageSet<? extends StorageMetadata> candiates = this.list(container, options);

        long result = candiates.stream().filter(sm -> sm != null && sm.getType() == StorageType.BLOB).count();
        return result;
    }

    @Override
    public BlobMetadata blobMetadata(String container, String name) {
        LOGGER.info("Get blob metadata of {}/{}", container, name);
        Blob blob = getBlob(container, name, null);
        if (blob != null) {
            blob.resetPayload(true);
            return blob.getMetadata();
        }
        return null;
    }

    @Override
    public Blob getBlob(String container, String name) {
        return getBlob(container, name, GetOptions.NONE);
    }

    @Override
    public String putBlob(String container, Blob blob) {
        return putBlob(container, blob, PutOptions.NONE);
    }

    @Override
    public void clearContainer(String container) {
        clearContainer(container, ListContainerOptions.Builder.recursive());
    }

    @Override
    public void clearContainer(String container, ListContainerOptions options) {
        LOGGER.info("Clear container {} with options {}", container, options);

        ListContainerOptions opts = options.clone();
        do {
            final PageSet<? extends StorageMetadata> pageSet = this.list(container, opts);
            opts = pageSet.getNextMarker() != null ? opts.afterMarker(pageSet.getNextMarker()) : opts;

            final List<String> keys = pageSet.stream().map(smd -> smd.getName()).collect(Collectors.toList());
            this.removeBlobs(container, keys);
        } while (opts.getMarker() != null);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container) {
        return this.list(container, ListContainerOptions.NONE);
    }

    @Override
    public void removeBlobs(String container, Iterable<String> names) {
        LOGGER.info("Remove blobs {} in container {}", names, container);

        for (String name : names) {
            this.removeBlob(container, name);
        }
    }

    @Override
    public void createDirectory(String container, String directory) {
        LOGGER.info("Create directory {} in container {}", directory, container);
        this.putBlob(container, this.blobBuilder(directory).type(StorageType.FOLDER).build());
    }

    @Override
    public void deleteDirectory(String containerName, String directory) {
        LOGGER.info("Delete directory {} in container {}", directory, containerName);
        this.removeBlob(containerName, directory);
    }

    @Override
    public boolean directoryExists(String container, String directory) {
        LOGGER.info("Check if directory {} exists in container {}", directory, container);
        return this.blobExists(container, directory);
    }

}
