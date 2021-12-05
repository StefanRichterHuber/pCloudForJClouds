package com.github.stefanrichterhuber.pCloudForjClouds.reference;

public class PCloudConstants {
	/**
	 * Specify the base directory where provider starts its file operations - must
	 * exists
	 */
	public static final String PROPERTY_BASEDIR = "jclouds.pcloud.basedir";
	
	/**
	 * Client secret to authenticate user
	 */
	public static final String PROPERTY_CLIENT_SECRET = "jclouds.pcloud.clientsecret";

	private PCloudConstants() {
		throw new AssertionError("intentionally unimplemented");
	}
}
