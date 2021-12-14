package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops;

/**
 * Result of the checksume calculation for an open file
 * @author Stefan Richter-Huber
 *
 */
public interface PCloudFileChecksum {
	String sha1();

	String md5();

	long size();
}
