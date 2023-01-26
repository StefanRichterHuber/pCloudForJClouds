package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import java.util.Map;
import java.util.Objects;

import com.google.gson.annotations.Expose;

/**
 * Externalized metadata of a blob
 * 
 * @author stefan
 *
 */
public class ExternalBlobMetadata {
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

    public ExternalBlobMetadata(String container, String key, BlobHashes hashes, Map<String, String> customMetadata) {
        super();
        this.hashes = hashes;
        this.customMetadata = customMetadata;
        this.container = container;
        this.key = key;
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

}