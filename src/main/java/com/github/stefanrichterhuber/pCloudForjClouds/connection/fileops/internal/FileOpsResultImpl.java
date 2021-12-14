package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal;

import com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal.PCloudError;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

class FileOpsResultImpl {
	@Expose
	@SerializedName("result")
	private int result;

	/**
	 * A value != 0 indicates an error
	 */

	public int result() {
		return this.result;
	}

	public PCloudError toError() {
		for (PCloudError pce : PCloudError.values()) {
			if (result == pce.getCode()) {
				return pce;
			}
		}
		return null;
	}
}
