package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops;

import java.io.IOException;

import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal.PCloudFileOpsImpl;
import com.pcloud.sdk.ApiClient;

/**
 * Represents low level operations with a pCloud file
 * 
 * @author Stefan Richter-Huber
 * @see https://docs.pcloud.com/methods/fileops/
 */
public interface PCloudFileOps {

	/**
	 * Flags used for {@link PCloudFileOps#open(long, Flag...)}
	 * @author Stefan Richter-Huber
	 *
	 */
	public enum Flag {
		WRITE(0x0002),
		CREATE(0x0040),
		EXCLUDE(0x0080),
		TRUNCATE(0x0200),
		APPEND(0x0400);
		private final int value;
		private Flag(int value) {
			this.value = value;
		}
		public int getValue() {
			return value;
		}
	}
	
	/**
	 * Opens a file in the folder with the given id
	 * @param parentFolder ID of the parent folder
	 * @param fileName Name of the file
	 * @param flags {@link Flag}s for opening the file
	 * @return {@link PCloudFileDescriptor} to access the file
	 * @throws IOException
	 */
	PCloudFileDescriptor open(long parentFolder, String fileName, Flag...flags) throws IOException;
	
	/**
	 * Opens the file with the given unique file id
	 * @param fileid ID of the file
	 * @param flags {@link Flag}s for opening the file
	 * @return {@link PCloudFileDescriptor} to access the file
	 * @throws IOException
	 */
	PCloudFileDescriptor open(long fileid, Flag... flags) throws IOException;
	
	/**
	 * Creates an instance of {@link PCloudFileOps}.
	 * @param apiClient {@link ApiClient} for basic connection properties.
	 * @return PCloudFileOps instance
	 */
	static PCloudFileOps create(ApiClient apiClient) {
		return new PCloudFileOpsImpl(apiClient);
	}
}
