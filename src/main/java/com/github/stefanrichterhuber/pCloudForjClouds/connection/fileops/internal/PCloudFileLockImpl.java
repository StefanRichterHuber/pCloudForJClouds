package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal;

import java.io.IOException;

import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileDescriptor;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileLock;

class PCloudFileLockImpl
		implements PCloudFileLock {

	protected PCloudFileOpsImpl fileOps;
	
	public PCloudFileLockImpl(PCloudFileOpsImpl fileOps, PCloudFileDescriptor parent) {
		super();
		this.fileOps = fileOps;
		this.parent = parent;
	}

	protected PCloudFileDescriptor parent;
	
	@Override
	public void close() throws IOException {
		this.fileOps.lock(parent.fd(), 0, false);

	}

	@Override
	public PCloudFileDescriptor getFile() {
		return parent;
	}
	

}
