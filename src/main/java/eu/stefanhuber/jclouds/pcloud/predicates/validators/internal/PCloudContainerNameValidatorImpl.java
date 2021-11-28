package eu.stefanhuber.jclouds.pcloud.predicates.validators.internal;

import com.google.inject.Singleton;

import eu.stefanhuber.jclouds.pcloud.predicates.validators.PCloudContainerNameValidator;

@Singleton
public class PCloudContainerNameValidatorImpl extends PCloudContainerNameValidator {

	@Override
	public void validate(String name) throws IllegalArgumentException {
		// container name cannot be null or empty
		if (name == null || name.length() < 1)
			throw new IllegalArgumentException("Container name can't be null or empty");

		// container name cannot contains / (or \ in Windows) character
		if (name.contains("\\") || name.contains("/"))
			throw new IllegalArgumentException("Container name '" + name + "' cannot contain \\ or /");
	}
}
