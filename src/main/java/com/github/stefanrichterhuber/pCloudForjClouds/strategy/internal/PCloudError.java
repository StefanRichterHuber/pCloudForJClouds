package com.github.stefanrichterhuber.pCloudForjClouds.strategy.internal;

import com.pcloud.sdk.ApiError;

/**
 * PCloud error codes 
 * @author Stefan Richter-Huber
 * @see https://docs.pcloud.com/errors/
 *
 */
enum PCloudError {
	LOGIN_REQUIRED(1000), // Log in required.
	NO_FULL_ID_PROVIED(1001), // No full path or name/folderid provided.
	NO_ID_PROVIDED(1004), // No fileid or path provided.
	NO_FLAGS_PROVIDED(1006), // Please provide flags.
	LOGIN_FAILED(2000), // Log in failed.
	INVALID_FILENAME(2001), // Invalid file/folder name.
	PARENT_DIR_DOES_NOT_EXISTS(2002), // A component of parent directory does not exist.
	ACCESS_DENIED(2003), // Access denied. You do not have permissions to preform this operation.
	ALREADY_EXISTS(2004), // File or folder alredy exists.
	DIRECTORY_DOES_NOT_EXIST(2005), // Directory does not exist.
	FOLDER_NOT_EMPTY(2006), //
	USER_OVER_QUOTA(2008), // User is over quota.
	FILE_NOT_FOUND(2009), // File not found.
	INVALID_PATH(2010), // Invalid path.
	FILE_OR_FOLDER_NOT_FOUND(2055), // File or folder not found
	TO_MANY_LOGIN_ATTEMPTS(4000), // Too many login tries from this IP address.
	INTERNAL_ERROR(5000), // Internal error. Try again later.
	INTERNAL_UPLOAD_ERROR(5001), // Internal upload error.
	UNKNOWN(-1);

	private final int code;

	private PCloudError(int code) {
		this.code = code;
	}
	
	public int getCode() {
		return this.code;
	}

	/**
	 * Parse an {@link ApiError} code 
	 * @param e {@link ApiError} to parse
	 * @return {@link PCloudError} found.
	 */
	public static PCloudError parse(ApiError e) {
		for(PCloudError pce: PCloudError.values()) {
			if(e.errorCode() == pce.code) {
				return pce;
			}
		}
		return UNKNOWN;
	}
}