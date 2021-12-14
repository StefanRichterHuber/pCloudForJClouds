package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

class LockResultImpl extends FileOpsResultImpl {
	@Expose
	@SerializedName("locked")
	private boolean locked;
	
	public boolean locked() {
		return this.locked;
	}
}
