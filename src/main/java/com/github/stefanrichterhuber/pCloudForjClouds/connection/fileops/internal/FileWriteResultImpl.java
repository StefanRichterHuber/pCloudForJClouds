package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

class FileWriteResultImpl extends FileOpsResultImpl {
	@Expose
    @SerializedName("bytes")
    private long bytes;
	
	
	public long bytes() {
		return bytes;
	}
}
