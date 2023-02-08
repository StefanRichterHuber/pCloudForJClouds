package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops;

/**
 * Result of the checksume calculation for an open file. Actually filled
 * checksums depends on the location of the pCloud store. European customers get
 * SHA-1 and SHA-256, global customers get MD5 and SHA1.
 * 
 * @author Stefan Richter-Huber
 *
 */
public interface PCloudFileChecksum {
    String sha1();

    String md5();

    String sha256();

    long size();
}
