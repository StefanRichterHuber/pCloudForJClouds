package com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.internal;

import com.github.stefanrichterhuber.pCloudForjClouds.predicates.validators.PCloudBlobKeyValidator;
import com.google.inject.Singleton;

@Singleton
public class PCloudBlobKeyValidatorImpl extends PCloudBlobKeyValidator {
	@Override
	public void validate(String name) throws IllegalArgumentException {
		// blob key cannot be null or empty
		if (name == null || name.length() < 1)
			throw new IllegalArgumentException("Blob key can't be null or empty");

		// blobkey cannot start with / (or \ in Windows) character
		if (name.startsWith("\\") || name.startsWith("/"))
			throw new IllegalArgumentException("Blob key '" + name + "' cannot start with \\ or /");
	}

}
