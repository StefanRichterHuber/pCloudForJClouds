package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

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
        final BlobHashes.Builder hashBuilder = new BlobHashes.Builder();
        try (final InputStream src = hashBuilder.wrap(new BufferedInputStream(this.blob.getPayload().openStream()));
                Source source = Okio.source(src)) {
            sink.writeAll(source);
            hashes = hashBuilder.toBlobHashes(null);
        }
    }

    public BlobHashes getHashes() {
        return this.hashes;
    }

    public HashingBlobDataSource(Blob blob) {
        super(blob);
    }

}
