package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import org.jclouds.io.payloads.ByteArrayPayload;

import com.google.common.hash.Hashing;

/**
 * Empty payload
 * @author Stefan Richter-Huber
 *
 */
public final class EmptyPayload extends ByteArrayPayload{
	private static final byte[] EMPTY_CONTENT = new byte[0];
	private static final byte[] EMPTY_CONTENT_MD5 = Hashing.md5().hashBytes(EMPTY_CONTENT).asBytes();

	public EmptyPayload() {
		super(EMPTY_CONTENT, EMPTY_CONTENT_MD5);
	}

}
