package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.domain.StorageType;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import com.google.gson.stream.JsonReader;
import com.pcloud.sdk.RemoteFile;

/**
 * Externalized metadata of a blob
 * 
 * @author stefan
 *
 */
public class ExternalBlobMetadata implements Comparable<ExternalBlobMetadata> {
    private static final Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

    /**
     * Empty metadata
     */
    public static final ExternalBlobMetadata EMPTY_METADATA = new ExternalBlobMetadata(null, null, 0, null,
            BlobAccess.PRIVATE,
            BlobHashes.empty(),
            Collections.emptyMap());

    /**
     * Different hashes of the target file
     */
    @Expose
    private BlobHashes hashes;

    /**
     * User defined metadata of the target file
     */
    @Expose
    @Nonnull
    private Map<String, String> customMetadata;

    /**
     * Container containing the blob
     */
    @Expose
    @Nonnull
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

    /**
     * Access type (public / private)
     */
    @Expose
    private BlobAccess access;

    /**
     * Type of blob
     */
    @Expose
    private StorageType storageType;

    public ExternalBlobMetadata(@Nonnull String container, String key, long id, StorageType storageType,
            BlobAccess access,
            BlobHashes hashes,
            Map<String, String> customMetadata) {
        super();
        this.hashes = hashes;
        this.customMetadata = customMetadata != null ? customMetadata : Collections.emptyMap();
        this.container = container;
        this.key = key;
        this.fileId = id;
        this.access = access;
        this.storageType = storageType;
    }

    public BlobHashes hashes() {
        return hashes;
    }

    @Nonnull
    public Map<String, String> customMetadata() {
        return customMetadata;
    }

    @Nonnull
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

    public StorageType storageType() {
        return this.storageType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(container, customMetadata, hashes, key);
    }

    /**
     * Reads a JSON representation of a {@link ExternalBlobMetadata} object.
     * 
     * @param json
     * @return
     */
    public static ExternalBlobMetadata fromJSON(String json) {
        if (json != null) {
            ExternalBlobMetadata md = setDefaults(gson.fromJson(json, ExternalBlobMetadata.class));
            return md;
        } else {
            return null;
        }
    }

    /**
     * Reads the {@link ExternalBlobMetadata} from a {@link RemoteFile} containing a
     * JSON representation
     * 
     * @param rf {@link RemoteFile} to read
     * @return {@link ExternalBlobMetadata} found
     * @throws IOException
     */
    public static ExternalBlobMetadata readJSON(RemoteFile rf) throws IOException {
        try (final InputStream is = rf.byteStream();
                final JsonReader reader = new JsonReader(
                        new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8)))) {
            final ExternalBlobMetadata result = setDefaults(gson.fromJson(reader, ExternalBlobMetadata.class));
            return result;
        }
    }

    /**
     * Converts this {@link ExternalBlobMetadata} to its JSON representation
     * 
     * @return
     */
    public String toJson() {
        final String v = gson.toJson(this);
        return v;
    }

    /**
     * Fix metadata written in previous versions and set reasonable defaults
     */
    private static ExternalBlobMetadata setDefaults(ExternalBlobMetadata md) {
        if (md != null) {
            // For older data, sometimes the storage type is missing, set to default
            if (md.storageType() == null) {
                md = new ExternalBlobMetadata(md.container(), md.key(), md.fileId(), StorageType.BLOB,
                        md.access(), md.hashes(), md.customMetadata());
            }
        }
        return md;
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
        return this.toJson();
    }

    @Override
    public int compareTo(ExternalBlobMetadata o) {
        if (this.container().equals(o.container())) {

            if (this.key() == null) {
                return -1;
            }
            return this.key().compareTo(o.key());
        }
        return this.container().compareTo(o.container());
    }

}