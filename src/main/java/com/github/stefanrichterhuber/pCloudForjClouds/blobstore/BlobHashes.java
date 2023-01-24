package com.github.stefanrichterhuber.pCloudForjClouds.blobstore;

import static com.google.common.io.BaseEncoding.base16;

import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

import com.google.common.hash.Hashing;
import com.google.gson.annotations.Expose;

public class BlobHashes {
    /**
     * Helps to create {@link BlobHashes}, especially by wrapping
     * {@link InputStream}s with {@link DigestInputStream}s in the
     * {@link #wrap(InputStream)} method.
     * 
     * @author stefan
     *
     */
    public static class Builder {
        private final MessageDigest md5;
        private final MessageDigest sha1;
        private final MessageDigest sha256;

        public Builder() {
            try {
                md5 = MessageDigest.getInstance("MD5");
                sha1 = MessageDigest.getInstance("SHA-1");
                sha256 = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Wraps {@link DigestInputStream}s for all required {@link MessageDigest}s
         * around the
         * given {@link InputStream}
         * 
         * @param inp {@link InputStream} to wrap
         * @return Modified {@link InputStream}.
         */
        public InputStream wrap(InputStream inp) {
            InputStream is = inp;
            for (MessageDigest md : Arrays.asList(md5, sha1, sha256)) {
                is = new DigestInputStream(is, md);
            }
            return is;
        }

        /**
         * Creates a {@link BlobHashes} object from the collected digests and the given
         * buildin hash
         * 
         * @param buildin
         * @return
         */
        public BlobHashes toBlobHashes(String buildin) {
            final String md5Hash = base16().lowerCase().encode(this.md5.digest());
            final String sha1Hash = base16().lowerCase().encode(this.sha1.digest());
            final String sha256Hash = base16().lowerCase().encode(this.sha256.digest());
            final BlobHashes hashes = new BlobHashes(md5Hash, sha1Hash, sha256Hash, buildin);
            return hashes;
        }

    }

    @SuppressWarnings("deprecation")
    private static final byte[] EMPTY_MD5 = Hashing.md5().hashBytes(new byte[0]).asBytes();

    @SuppressWarnings("deprecation")
    private static final byte[] EMPTY_SHA1 = Hashing.sha1().hashBytes(new byte[0]).asBytes();

    private static final byte[] EMPTY_SHA256 = Hashing.sha256().hashBytes(new byte[0]).asBytes();

    private static final String EMPTY_MD5_B16 = base16().lowerCase().encode(EMPTY_MD5);

    private static final String EMPTY_SHA1_B16 = base16().lowerCase().encode(EMPTY_SHA1);

    private static final String EMPTY_SHA256_B16 = base16().lowerCase().encode(EMPTY_SHA256);

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
        return md5() != null && !"".equals(md5())
                && buildin() != null && !"".equals(buildin())
                && sha1() != null && !"".equals(sha1())
                && sha256() != null && !"".equals(sha256());
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
     * Creates an empty {@link BlobHashes} object.
     * 
     * @return
     */
    public static BlobHashes empty() {
        return new BlobHashes(EMPTY_MD5_B16, EMPTY_SHA1_B16, EMPTY_SHA256_B16, null);
    }

    /**
     * Creates a new {@link BlobHashes} instance with a new buildin hash
     * 
     * @param buildin
     * @return new {@link BlobHashes} instance.
     */
    public BlobHashes withBuildin(String buildin) {
        return new BlobHashes(md5(), sha1(), sha256(), buildin);
    }

    @Override
    public int hashCode() {
        return Objects.hash(buildin(), md5(), sha1(), sha256());
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
        return Objects.equals(buildin(), other.buildin()) //
                && Objects.equals(md5(), other.md5()) //
                && Objects.equals(sha1(), other.sha1()) //
                && Objects.equals(sha256(), other.sha256());
    }

    @Override
    public String toString() {
        return "BlobHashes [md5=" + md5() + ", sha1=" + sha1() + ", sha256=" + sha256() + ", buildin=" + buildin()
                + "]";
    }
}