package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

 class FileSizeResultImpl extends FileOpsResultImpl {
	@Expose
	@SerializedName("size")
	private long size;
	
	@Expose
	@SerializedName("offset")
	private long offset;
	
	public long size() {
		return this.size;
	}
	
	public long offset() {
		return this.offset;
	}
}
