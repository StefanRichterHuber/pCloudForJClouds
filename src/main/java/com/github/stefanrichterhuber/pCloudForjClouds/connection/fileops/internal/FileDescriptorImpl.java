package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileChecksum;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileDescriptor;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileLock;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

class FileDescriptorImpl extends FileOpsResultImpl implements AutoCloseable, PCloudFileDescriptor {
    @Expose
    @SerializedName("fileid")
    private long fileId;

    @Expose
    @SerializedName("fd")
    private int fd;

    private boolean closed = false;
    private PCloudFileOpsImpl fileOps;

    public int fd() {
        return this.fd;
    }

    public long fileid() {
        return fileId;
    }

    protected void setFileOps(PCloudFileOpsImpl fileOps) {
        this.fileOps = fileOps;
    }

    private void assertNotClosed() {
        if (this.closed) {
            throw new IllegalStateException("File descriptor already closed");
        }
    }

    public long write(byte[] content) throws IOException {
        assertNotClosed();
        return this.fileOps.write(fd, content);
    }

    public long write(long fileOffset, byte[] content) throws IOException {
        assertNotClosed();
        return this.fileOps.write(fd, fileOffset, content);
    }

    @Override
    public long write(byte[] content, int offset, int len) throws IOException {
        assertNotClosed();
        if (content == null) {
            throw new NullPointerException();
        } else if ((offset < 0) || (offset > content.length) || (len < 0) || ((offset + len) > content.length)
                || ((offset + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        return this.write(Arrays.copyOfRange(content, offset, offset + len));
    }

    public long write(long fileOffset, byte[] content, int offset, int len) throws IOException {
        assertNotClosed();
        if (content == null) {
            throw new NullPointerException();
        } else if ((offset < 0) || (offset > content.length) || (len < 0) || ((offset + len) > content.length)
                || ((offset + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        return this.write(fileOffset, Arrays.copyOfRange(content, offset, offset + len));
    }

    public void close() throws IOException {
        if (!this.closed) {
            this.closed = this.fileOps.close(fd);
        }
    }

    /**
     * Creates an {@link OutputStream} for this file. Buffering is heavily
     * recommended since every write is directly forwarded to the backend! Closing
     * the Stream also closes the file descriptor!!
     * 
     * @return
     */
    public OutputStream openStream() {
        assertNotClosed();
        return new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                FileDescriptorImpl.this.write(new byte[] { (byte) b });
            }

            public void write(byte b[]) throws IOException {
                FileDescriptorImpl.this.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                FileDescriptorImpl.this.write(b, off, len);
            }

            @Override
            public void close() throws IOException {
                FileDescriptorImpl.this.close();
            }
        };
    }

    @Override
    public PCloudFileLock lock(LockType type, boolean noBlock) throws IOException {
        assertNotClosed();
        boolean lock = this.fileOps.lock(fd, type == LockType.SHARED_LOCK ? 1 : 2, noBlock);
        if (lock) {
            return new PCloudFileLockImpl(fileOps, this);
        } else {
            return null;
        }
    }

    @Override
    public long size() throws IOException {
        assertNotClosed();
        return this.fileOps.getSize(this.fd);
    }

    @Override
    public PCloudFileChecksum calculateChecksum(long offset, long count) throws IOException {
        return this.fileOps.calculateChecksum(fd, offset, count);
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public long read(byte[] target, long offset, int len) throws IOException {
        return this.fileOps.read(fd, target, offset, len);
    }

    @Override
    public void truncate(long size) throws IOException {
        this.fileOps.truncate(fd, size);
    }
}
