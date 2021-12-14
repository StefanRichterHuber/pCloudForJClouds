package com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal;

import java.io.IOException;
import java.io.InputStream;

import org.jclouds.blobstore.config.LocalBlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.io.MutableContentMetadata;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.BasePayload;

import com.pcloud.sdk.RemoteFile;

/**
 * {@link Payload} from a PCloud {@link RemoteFile}.
 * 
 * @author Stefan Richter-Huber
 *
 */
public class RemoteFilePayload extends BasePayload<RemoteFile> {
	/**
	 * Factory interface for lazy openning streams.
	 * @author Stefan Richter-Huber
	 *
	 */
	@FunctionalInterface
	private static interface InputStreamFactory {
		InputStream openStream() throws IOException;
	}

	/**
	 * This is a workaround for the fact that
	 * {@link LocalBlobStore#getBlob(String, String, org.jclouds.blobstore.options.GetOptions)}
	 * creates a copy (with {@link LocalBlobStore#copyBlob(Blob blob)} of the blob
	 * when the {@link GetOptions} are non-null. This copy unfortunately always
	 * opens an {@link InputStream}. This is a rather expensive operation for pCloud
	 * {@link RemoteFile}s and there is no guarantee that this {@link InputStream}
	 * is closed, leaving dangling connections to the pcloud servers.
	 * 
	 * @author Stefan Richter-Huber
	 *
	 */
	private static class LazyInputStream extends InputStream {
		private final InputStreamFactory isf;
		private InputStream delegate;

		public LazyInputStream(InputStreamFactory isf) {
			super();
			this.isf = isf;
		}

		private InputStream getDelegate() throws IOException {
			return delegate == null ? (delegate = isf.openStream()) : delegate;
		}

		public void close() throws IOException {
			if (delegate != null) {
				delegate.close();
			}
		}

		public int read() throws IOException {
			return getDelegate().read();
		}

		public int hashCode() {
			try {
				return getDelegate().hashCode();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public int read(byte[] b) throws IOException {
			return getDelegate().read(b);
		}

		public boolean equals(Object obj) {

			try {
				return getDelegate().equals(obj);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public int read(byte[] b, int off, int len) throws IOException {
			return getDelegate().read(b, off, len);
		}

		public long skip(long n) throws IOException {
			return getDelegate().skip(n);
		}

		public String toString() {
			try {
				return getDelegate().toString();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public int available() throws IOException {
			return getDelegate().available();
		}

		public void mark(int readlimit) {
			try {
				getDelegate().mark(readlimit);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		public void reset() throws IOException {
			getDelegate().reset();
		}

		public boolean markSupported() {
			try {
				return getDelegate().markSupported();
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

	}

	public RemoteFilePayload(RemoteFile content) {
		super(content);
	}

	public RemoteFilePayload(RemoteFile content, MutableContentMetadata contentMetadata) {
		super(content, contentMetadata);
	}

	@Override
	public InputStream openStream() throws IOException {
		return new LazyInputStream(() -> this.content.byteStream());
	}

}
