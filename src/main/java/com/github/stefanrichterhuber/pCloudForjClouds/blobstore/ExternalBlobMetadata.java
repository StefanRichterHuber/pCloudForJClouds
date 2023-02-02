package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import java.util.Map;
import java.util.Objects;

import org.jclouds.blobstore.domain.BlobAccess;

import com.google.gson.annotations.Expose;

/**
 * Externalized metadata of a blob
 * 
 * @author stefan
 *
 */
public class ExternalBlobMetadata implements Comparable<ExternalBlobMetadata> {
    /**
     * Different hashes of the target file
     */
    @Expose
    private BlobHashes hashes;

    /**
     * User defined metadata of the target file
     */
    @Expose
    private Map<String, String> customMetadata;

    /**
     * Container containing the blob
     */
    @Expose
    private String container;

    /**
     * Key of the blob
     */
    @Expose
    private String key;

    /**
     * File id of the blob
     */
    @Expose
    private long fileId;

    @Expose
    private BlobAccess access;

    public ExternalBlobMetadata(String container, String key, long id, BlobAccess access, BlobHashes hashes,
            Map<String, String> customMetadata) {
        super();
        this.hashes = hashes;
        this.customMetadata = customMetadata;
        this.container = container;
        this.key = key;
        this.fileId = id;
        this.access = access;
    }

    public BlobHashes hashes() {
        return hashes;
    }

    public Map<String, String> customMetadata() {
        return customMetadata;
    }

    public String container() {
        return this.container;
    }

    public String key() {
        return this.key;
    }

    public long fileId() {
        return this.fileId;
    }

    public BlobAccess access() {
        return this.access;
    }

    @Override
    public int hashCode() {
        return Objects.hash(container, customMetadata, hashes, key);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ExternalBlobMetadata other = (ExternalBlobMetadata) obj;
        return Objects.equals(container, other.container) && Objects.equals(customMetadata, other.customMetadata)
                && Objects.equals(hashes, other.hashes) && Objects.equals(key, other.key);
    }

    @Override
    public String toString() {
        return "ExternalBlobMetadata [hashes=" + hashes + ", customMetadata=" + customMetadata + ", container="
                + container + ", key=" + key + "]";
    }

    @Override
    public int compareTo(ExternalBlobMetadata o) {
        if (this.container().equals(o.container())) {
            return this.key().compareTo(o.key());
        }
        return this.container().compareTo(o.container());
    }

}