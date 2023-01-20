package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import static com.google.common.io.BaseEncoding.base16;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.jclouds.blobstore.domain.Blob;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.BlobHashes;

import okio.BufferedSink;
import okio.Okio;
import okio.Source;

/**
 * {@link BlobDataSource} also computing several hashes during transfering data
 * from {@link Blob} to {@link BufferedSink}.
 * 
 * @author stefan
 *
 */
public class HashingBlobDataSource extends BlobDataSource {

    private BlobHashes hashes;

    public void writeTo(BufferedSink sink) throws IOException {

        try (final InputStream src = new BufferedInputStream(this.blob.getPayload().openStream());
                DigestInputStream sh256digest = new DigestInputStream(src,
                        MessageDigest.getInstance("SHA-256")); //
                DigestInputStream sh1digest = new DigestInputStream(sh256digest,
                        MessageDigest.getInstance("SHA-1")); //
                DigestInputStream md5digest = new DigestInputStream(sh1digest,
                        MessageDigest.getInstance("MD5")); //

                Source source = Okio.source(md5digest)) {
            sink.writeAll(source);

            final String md5 = base16().lowerCase().encode(md5digest.getMessageDigest().digest());
            final String sha1 = base16().lowerCase().encode(sh1digest.getMessageDigest().digest());
            final String sha256 = base16().lowerCase().encode(sh256digest.getMessageDigest().digest());
            hashes = new BlobHashes(md5, sha1, sha256, null);
        } catch (NoSuchAlgorithmException e) {
            throw new IOException(e);
        }
    }

    public BlobHashes getHashes() {
        return this.hashes;
    }

    public HashingBlobDataSource(Blob blob) {
        super(blob);
    }

}
