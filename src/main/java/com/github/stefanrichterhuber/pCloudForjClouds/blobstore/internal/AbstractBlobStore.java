package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

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

/**
 * Some provider independent implementations of {@link BlobStore}.
 * 
 * @author Stefan Richter-Huber
 *
 */
public abstract class AbstractBlobStore implements BlobStore {
    @Override
    public void downloadBlob(String container, String name, File destination) {
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
        final PageSet<? extends StorageMetadata> candiates = this.list(container, options);
        long result = 0;
        for (StorageMetadata candiate : candiates) {
            if (candiate.getType() == StorageType.BLOB) {
                result++;
            }
        }
        return result;
    }

    @Override
    public BlobMetadata blobMetadata(String container, String name) {
        Blob blob = getBlob(container, name, null);
        return blob != null ? blob.getMetadata() : null;
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
    public PageSet<? extends StorageMetadata> list(String container) {
        return this.list(container, ListContainerOptions.NONE);
    }

    @Override
    public void removeBlobs(String container, Iterable<String> names) {
        for (String name : names) {
            this.removeBlob(container, name);
        }
    }
}
