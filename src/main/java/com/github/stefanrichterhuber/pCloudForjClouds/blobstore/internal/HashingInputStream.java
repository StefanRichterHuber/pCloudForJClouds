package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

/**
 * {@link InputStream} proxy to create the a hash of all data loaded from the delegate {@link InputStream}.
 */
public class HashingInputStream extends InputStream {
    private final InputStream delegate;
    private final MessageDigest md;

    public HashingInputStream(InputStream delegate, MessageDigest md) {
        this.delegate = delegate;
        this.md = md;
    }

    public MessageDigest getMessageDigest() {
        return this.md;
    }

    @Override
    public int read() throws IOException {
        final int d = delegate.read();
        if (d != -1) {
            this.md.update((byte) (d & 0xff));
        }
        return d;
    }

    public int read(byte[] buf) throws java.io.IOException {
        return read(buf, 0, buf.length);
    }

    public int read(byte[] arg0, int arg1, int arg2) throws java.io.IOException {
        final int result = this.delegate.read(arg0, arg1, arg2);
        if (result != -1) {
            this.md.update(arg0, arg1, arg2);
        }
        return result;
    }

    public byte[] readAllBytes() throws java.io.IOException {
        final byte[] result = this.delegate.readAllBytes();
        this.md.update(result);
        return result;
    }

    public byte[] readNBytes(int arg0) throws java.io.IOException {
        final byte[] result = this.delegate.readNBytes(arg0);
        this.md.update(result);
        return result;
    }

    public int readNBytes(byte[] arg0, int arg1, int arg2) throws java.io.IOException {
        final int result = this.delegate.readNBytes(arg0, arg1, arg2);
        if (result != -1) {
            this.md.update(arg0, arg1, arg2);
        }
        return result;
    }

    public long skip(long arg0) throws java.io.IOException {
        return this.delegate.skip(arg0);
    }

    public void skipNBytes(long arg0) throws java.io.IOException {
        this.delegate.skipNBytes(arg0);
    }

    public int available() throws java.io.IOException {
        return this.delegate.available();
    }

    public void close() throws java.io.IOException {
        this.delegate.close();
    }

    public synchronized void mark(int arg0) {
        this.delegate.mark(arg0);
    }

    public synchronized void reset() throws java.io.IOException {
        this.delegate.reset();
    }

    public boolean markSupported() {
        return this.delegate.markSupported();
    }

    public long transferTo(java.io.OutputStream arg0) throws java.io.IOException {
        final byte[] result = delegate.readAllBytes();
        this.md.update(result);
        arg0.write(result);
        return result.length;
    }
}
