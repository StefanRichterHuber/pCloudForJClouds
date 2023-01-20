package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.jclouds.blobstore.domain.Blob;

import com.pcloud.sdk.DataSource;

import okio.BufferedSink;
import okio.Okio;
import okio.Source;

/**
 * PCloud {@link DataSource} created from a {@link Blob}.
 * 
 * @author Stefan Richter-Huber
 *
 */
public class BlobDataSource extends DataSource {
    protected final Blob blob;

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
        try (Source source = Okio.source(blob.getPayload().openStream())) {
            sink.writeAll(source);
        }
    }

    public long contentLength() {
        return blob.getMetadata().getContentMetadata().getContentLength();
    }

    public BlobDataSource(Blob blob) {
        super();
        this.blob = checkNotNull(blob, "blob");
    }

    @Override
    public String toString() {
        return "pCloud datasource from blob " + this.blob;
    }

}
