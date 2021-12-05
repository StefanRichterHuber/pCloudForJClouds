package com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal;

import org.jclouds.blobstore.BlobStore;

/**
 * Exception from PCloud {@link BlobStore}.
 * @author Stefan Richter-Huber
 *
 */
public class PCloudBlobStoreException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public PCloudBlobStoreException(Throwable cause) {
	        super(cause);
	    }
}
