package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal;

import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileChecksum;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;


class FileChecksumResultImpl extends FileOpsResultImpl implements PCloudFileChecksum {
	@Expose
	@SerializedName("sha1")
	private String sha1;
	
	@Expose
	@SerializedName("md5")
	private String md5;
	
	@Expose
	@SerializedName("size")
	private long size;
	
	public String sha1() {
		return this.sha1;
	}
	
	public String md5() {
		return this.md5;
	}
	
	public long size() {
		return this.size;
	}
}
