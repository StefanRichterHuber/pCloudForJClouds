package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.IOException;
import java.io.InputStream;

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

	public RemoteFilePayload(RemoteFile content) {
		super(content);
	}

	public RemoteFilePayload(RemoteFile content, MutableContentMetadata contentMetadata) {
		super(content, contentMetadata);
	}

	@Override
	public InputStream openStream() throws IOException {
		return this.content.byteStream();
	}

}
