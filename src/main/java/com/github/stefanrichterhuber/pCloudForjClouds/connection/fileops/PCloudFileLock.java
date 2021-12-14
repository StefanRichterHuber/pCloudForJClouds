package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops;

import java.io.Closeable;

public interface PCloudFileLock extends Closeable {
	PCloudFileDescriptor getFile();
}
