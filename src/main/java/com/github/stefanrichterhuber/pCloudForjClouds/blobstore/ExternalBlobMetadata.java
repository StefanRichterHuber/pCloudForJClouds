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

    public ExternalBlobMetadata(BlobHashes hashes, Map<String, String> customMetadata) {
        super();
        this.hashes = hashes;
        this.customMetadata = customMetadata;
    }

    public BlobHashes hashes() {
        return hashes;
    }

    public Map<String, String> customMetadata() {
        return customMetadata;
    }

    @Override
    public int hashCode() {
        return Objects.hash(customMetadata, hashes);
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
        return Objects.equals(customMetadata, other.customMetadata) && Objects.equals(hashes, other.hashes);
    }

    @Override
    public String toString() {
        return "ExternalBlobMetadata [hashes=" + hashes + ", customMetadata=" + customMetadata + "]";
    }

}