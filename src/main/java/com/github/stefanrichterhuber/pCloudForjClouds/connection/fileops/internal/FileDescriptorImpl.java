package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal;

import java.io.IOException;
import java.io.InputStream;
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

    public InputStream inputStream() {
        assertNotClosed();
        return new InputStream() {
            private static final int DEFAULT_BUFFER_SIZE = 8192;
            private long offset = 0;
            private long mark = 0;

            public int read() {
                byte[] b = new byte[1];
                try {
                    if (this.read(b) > 0) {
                        return (int) b[0];
                    } else {
                        return -1;
                    }
                } catch (IOException e) {
                    return -1;
                }
            }

            public byte[] readAllBytes() throws IOException {
                return readNBytes(Integer.MAX_VALUE);
            }

            public int read(byte b[]) throws IOException {
                return read(b, 0, b.length);
            }

            public int read(byte b[], int off, int len) throws IOException {
                if (len < 0) {
                    throw new IllegalArgumentException("len < 0");
                }
                if (off == 0) {
                    final long result = FileDescriptorImpl.this.read(b, offset, len);
                    if (result == 0) {
                        return -1;
                    }
                    this.offset += result;
                    return (int) result;
                } else {
                    int remaining = len;
                    int n;
                    int nread = 0;
                    do {
                        // Read next slice from input
                        byte[] buf = new byte[Math.min(remaining, DEFAULT_BUFFER_SIZE)];
                        n = this.read(buf);
                        if (n < 0) {
                            break;
                        }
                        nread += n;
                        remaining -= n;

                        // Copy buffer to target
                        System.arraycopy(buf, 0, b, off + nread, n);
                    } while (n > 0 && remaining > 0);
                    return nread;
                }
            }

            public int readNBytes(byte[] b, int off, int len) throws IOException {
                return this.read(b, off, len);
            }

            public long skip(long n) throws IOException {
                if (n > 0) {
                    long available = this.available();
                    if (available < n) {
                        offset += available;
                        return available;
                    } else {
                        offset += n;
                        return n;
                    }
                }
                return 0;
            }

            public void skipNBytes(long n) throws IOException {
                skip(n);
            }

            public int available() throws IOException {
                return (int) (FileDescriptorImpl.this.size() - offset);
            }

            public void close() throws IOException {
                FileDescriptorImpl.this.close();
            }

            public synchronized void mark(int readlimit) {
                this.mark = this.offset;
            }

            public synchronized void reset() throws IOException {
                this.offset = this.mark;
            }

            public boolean markSupported() {
                return true;
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
        assertNotClosed();
        return this.fileOps.calculateChecksum(fd, offset, count);
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    @Override
    public long read(byte[] target, long offset, int len) throws IOException {
        assertNotClosed();
        return this.fileOps.read(fd, target, offset, len);
    }

    @Override
    public void truncate(long size) throws IOException {
        assertNotClosed();
        this.fileOps.truncate(fd, size);
    }
}
