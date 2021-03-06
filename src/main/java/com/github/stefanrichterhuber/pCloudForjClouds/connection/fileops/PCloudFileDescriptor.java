package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops;

import java.io.IOException;
import java.io.OutputStream;

/**
 * The file descriptors are an abstract indicator for accessing a file. With
 * these descriptors low level operations are available for the files in pCloud
 * file system - create, read, write , etc.
 * 
 * @author Stefan Richter-Huber
 * @see https://docs.pcloud.com/methods/fileops/
 */
public interface PCloudFileDescriptor extends AutoCloseable {

	public enum LockType {
		SHARED_LOCK, EXCLUSIVE_LOCK
	}

	/**
	 * Raw file descriptor
	 * 
	 * @return
	 */
	int fd();

	/**
	 * Unique id of the file opened
	 * 
	 * @return
	 */
	long fileid();

	/**
	 * Writes the given content to the opened file
	 * 
	 * @param content Content to write
	 * @return  Number of bytes actually written
	 * @throws IOException
	 */
	long write(byte[] content) throws IOException;

	/**
	 * Writes the given content to the opened file
	 * 
	 * @param content Content to write
	 * @param offset  the start offset in the data.
	 * @param len     the number of bytes to write.
	 * @return Number of bytes actually written
	 * @throws IOException
	 */
	long write(byte[] content, int offset, int len) throws IOException;

	void close() throws IOException;

	long size() throws IOException;
	
	/**
	 * Is the file connection closed?
	 * @return closed
	 */
	boolean isClosed();

	/**
	 * Locks this file
	 * 
	 * @param type    Type of lock to acquire
	 * @param noBlock Block until block acquired?
	 * @return Lock state after the operation
	 * @throws IOException
	 */
	PCloudFileLock lock(LockType type, boolean noBlock) throws IOException;

	/**
	 * Calculates the checksum of the given part of the file
	 * 
	 * @param offset Offset
	 * @param count  Number of bytes from offest
	 * @return {@link PCloudFileChecksum} result.
	 * @throws IOException
	 */
	PCloudFileChecksum calculateChecksum(long offset, long count) throws IOException;

	/**
	 * Creates an {@link OutputStream} for this file. Buffering is heavily
	 * recommended since every write is directly forwarded to the backend! Closing
	 * the Stream also closes the file descriptor!
	 * 
	 * @return
	 */
	OutputStream openStream();
}
