package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.Base64;

import static com.google.common.base.Preconditions.checkNotNull;
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
	private final Blob blob;
	private final MessageDigest md;

	@Override
	public void writeTo(BufferedSink sink) throws IOException {
		final InputStream src = blob.getPayload().openStream();
		final InputStream is = md != null ? new DigestInputStream(src, md) : src;

		try (Source source = Okio.source(is)) {
			sink.writeAll(source);
		}
	}

	public String hash() {
		final byte[] h = md.digest();
		final String result = Base64.getEncoder().encodeToString(h);
		return result;
	}

	public long contentLength() {
		return blob.getMetadata().getContentMetadata().getContentLength();
	}

	public BlobDataSource(Blob blob, MessageDigest md) {
		super();
		this.blob = checkNotNull(blob, "blob");
		this.md = md;
	}

	@Override
	public String toString() {
		return "pCloud datasource from blob " + this.blob;
	}

}
