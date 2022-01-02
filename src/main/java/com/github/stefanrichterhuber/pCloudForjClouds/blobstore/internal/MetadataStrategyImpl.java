package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.BaseEncoding.base16;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.MetadataStrategy;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.RemoteFile;
import com.pcloud.sdk.RemoteFolder;
import com.pcloud.sdk.UploadOptions;

public class MetadataStrategyImpl implements MetadataStrategy {
	private static final Type GSON_MAP_TYPE = new TypeToken<Map<String, String>>() {
	}.getType();

	private static final Logger LOGGER = LoggerFactory.getLogger(MetadataStrategyImpl.class);

	private static final String SEPARATOR = "/";

	private final ApiClient apiClient;

	private final String baseDirectory;

	private final boolean active;

	private final String metadataFolder;

	private final RemoteFolder metadataRemoteFolder;

	private final Gson gson;

	@Inject
	protected MetadataStrategyImpl( //
			ApiClient apiClient, //
			@Named(PCloudConstants.PROPERTY_BASEDIR) String baseDir, //
			@Named(PCloudConstants.PROPERTY_USERMETADATA_ACTIVE) boolean active, //
			@Named(PCloudConstants.PROPERTY_USERMETADATA_FOLDER) String metadataFolder //
	) {
		this.baseDirectory = checkNotNull(baseDir, "Property " + PCloudConstants.PROPERTY_BASEDIR);
		this.active = active;
		this.apiClient = checkNotNull(apiClient, "PCloud api client");
		final String metadataFolderName = checkNotNull(metadataFolder,
				"Property " + PCloudConstants.PROPERTY_USERMETADATA_FOLDER);
		this.metadataFolder = this.baseDirectory + SEPARATOR + metadataFolderName;
		this.metadataRemoteFolder = this.active ? PCloudUtils.createBaseDirectory(this.apiClient, this.metadataFolder)
				: null;
		this.gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

		LOGGER.debug("User defined metadata stored at '{}'", metadataFolder);
	}

	/**
	 * Returns local instance of PCloud {@link ApiClient}.
	 * 
	 * @return {@link ApiClient}
	 */
	private ApiClient getApiClient() {
		return this.apiClient;
	}

	/**
	 * Builds the metadata file name for a given container and blob name.
	 * 
	 * @param container Name of the container
	 * @param key       Key of the blob
	 * @return Name of the metadata file.
	 */
	private String buildMetadataFileName(String container, String key) {
		return base16().lowerCase()
				.encode(Hashing.sha256().hashString(container + SEPARATOR + key, StandardCharsets.UTF_8).asBytes())
				+ ".metadata.json";
	}

	/**
	 * Full file path for the metadata file for a given container and blob name.
	 * 
	 * @param container Name of the container
	 * @param key       Key of the blob
	 * @return
	 */
	private String buildMetadataFilePath(String container, String key) {
		return metadataFolder + SEPARATOR + buildMetadataFileName(container, key);
	}

	@Override
	public CompletableFuture<Map<String, String>> get(String container, String key) {
		if (this.active) {
			final String path = buildMetadataFilePath(container, key);
			return PCloudUtils.execute(this.getApiClient().loadFile(path)) //
					.thenApplyAsync(this::readMetadata) //
					.thenApply(v -> {
						LOGGER.debug("Successfully loaded custom metadata for blob (metadata file {}) {}{}{}: {}", path,
								container, SEPARATOR, key, v);
						return v;
					}) //
					.exceptionally(e -> this.notFoundDefault(e, () -> Collections.emptyMap()));
		} else {
			return CompletableFuture.completedFuture(Collections.emptyMap());
		}
	}

	/**
	 * Reads the JSON representation of the metadata from the given file and parses
	 * it.
	 * 
	 * @param rf {@link RemoteFile} containing the metadata to read
	 * @return Metadata {@link Map}. Might be empty, but never null.
	 * @see https://www.baeldung.com/gson-json-to-map
	 */
	private Map<String, String> readMetadata(RemoteFile rf) {
		if (rf.size() == 0) {
			// File is empty -> no need to read it
			return Collections.emptyMap();
		}
		try (JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(rf.byteStream())))) {
			final Map<String, String> result = gson.fromJson(reader, GSON_MAP_TYPE);
			return result;
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
	}

	/**
	 * Utility method for exception handling, returning a default value if the
	 * requested file was not found.
	 * 
	 * @param <T>
	 * @param e
	 * @param defaultValue
	 * @return
	 */
	private <T> T notFoundDefault(Throwable e, Supplier<T> defaultValue) {
		if (e instanceof ApiError) {
			final PCloudError pCloudError = PCloudError.parse((ApiError) e);
			if (pCloudError == PCloudError.FILE_NOT_FOUND || pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND) {
				return defaultValue.get();
			}
		}
		if (e.getCause() instanceof ApiError) {
			final PCloudError pCloudError = PCloudError.parse((ApiError) e.getCause());
			if (pCloudError == PCloudError.FILE_NOT_FOUND || pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND) {
				return defaultValue.get();
			}
		}
		if (e instanceof RuntimeException) {
			throw (RuntimeException) e;
		}
		throw new RuntimeException(e);
	}

	@Override
	public CompletableFuture<Void> put(String container, String key, Map<String, String> metadata) {
		if (this.active) {
			if (metadata == null || metadata.isEmpty()) {
				// No (new) metadata present, just delete the old one if one is present.
				return this.delete(container, key);
			} else {
				final String name = buildMetadataFileName(container, key);
				return CompletableFuture.supplyAsync(() -> writeMetadata(metadata)) //
						.thenComposeAsync(ds -> PCloudUtils.execute(this.getApiClient()
								.createFile(this.metadataRemoteFolder, name, ds, UploadOptions.OVERRIDE_FILE)))
						.thenAccept(rf -> LOGGER.debug(
								"Successfully uploaded custom metadata for blob (metadata file {}) {}{}{}: {}", name,
								container, SEPARATOR, key, metadata));
			}
		} else {
			if (metadata != null && !metadata.isEmpty()) {
				LOGGER.warn(
						"Metadata support not active (property {}) but custom metadata part of the request for {}{}{}: {}",
						PCloudConstants.PROPERTY_USERMETADATA_ACTIVE, container, SEPARATOR, key, metadata);
			}
			return CompletableFuture.completedFuture(null);
		}
	}

	/**
	 * Creates a {@link DataSource} containing a json representation of the given
	 * metadata
	 * 
	 * @param metadata
	 * @return
	 */
	private DataSource writeMetadata(Map<String, String> metadata) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (Writer w = new OutputStreamWriter(bos, StandardCharsets.UTF_8)) {
			gson.toJson(metadata, GSON_MAP_TYPE, w);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return DataSource.create(bos.toByteArray());
	}

	@Override
	public CompletableFuture<Void> delete(String container, String key) {
		if (this.active) {
			final String path = buildMetadataFilePath(container, key);
			return PCloudUtils.execute(getApiClient().deleteFile(path)) //
					.exceptionally(e -> notFoundDefault(e, () -> true)) //
					.thenAccept(
							v -> LOGGER.debug("Successfully deleted custom metadata for blob (metadata file {}) {}{}{}",
									path, container, SEPARATOR, key));
		} else {
			return CompletableFuture.completedFuture(null);
		}
	}

}
