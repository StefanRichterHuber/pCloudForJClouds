package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import java.util.Objects;

import com.google.gson.annotations.Expose;

public class BlobHashes {
    @Expose
    private String md5;
    @Expose
    private String sha1;
    @Expose
    private String sha256;
    @Expose
    private String buildin;

    public BlobHashes(String md5, String sha1, String sha256, String buildin) {
        super();
        this.md5 = md5;
        this.sha1 = sha1;
        this.sha256 = sha256;
        this.buildin = buildin;
    }

    public boolean isValid() {
        return md5() != null && !"".equals(md5)
                && buildin != null && !"".equals(buildin)
                && sha1 != null && !"".equals(sha1)
                && sha256 != null && !"".equals(sha256);
    }

    public String md5() {
        return md5;
    }

    public String sha1() {
        return sha1;
    }

    public String sha256() {
        return sha256;
    }

    public String buildin() {
        return buildin;
    }

    /**
     * Creates a new {@link BlobHashes} instance with a new buildin hash
     * 
     * @param buildin
     * @return new {@link BlobHashes} instance.
     */
    public BlobHashes withBuildin(String buildin) {
        return new BlobHashes(md5, sha1, sha256, buildin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(buildin, md5, sha1, sha256);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BlobHashes other = (BlobHashes) obj;
        return Objects.equals(buildin, other.buildin) && Objects.equals(md5, other.md5)
                && Objects.equals(sha1, other.sha1) && Objects.equals(sha256, other.sha256);
    }

    @Override
    public String toString() {
        return "BlobHashes [md5=" + md5 + ", sha1=" + sha1 + ", sha256=" + sha256 + ", buildin=" + buildin + "]";
    }
}