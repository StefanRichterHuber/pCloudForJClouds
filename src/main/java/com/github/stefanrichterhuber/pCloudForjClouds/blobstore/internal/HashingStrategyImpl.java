package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.BaseEncoding.base16;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.inject.Inject;
import javax.inject.Named;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.gaul.shaded.org.eclipse.jetty.io.RuntimeIOException;
import org.jclouds.blobstore.domain.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.HashingStrategy;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.hash.Hashing;
import com.google.common.hash.HashingInputStream;
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

import okio.BufferedSink;
import okio.Okio;
import okio.Source;

public class HashingStrategyImpl implements HashingStrategy {

	private class AugementedBlobDataSource extends DataSource {
		private final Blob blob;
		private final MessageDigest mdM5;
		private final MessageDigest mdSHA1;
		private final Consumer<Hashes> afterWrite;

		public AugementedBlobDataSource(Blob blob, Consumer<Hashes> afterWrite) throws NoSuchAlgorithmException {
			this.blob = blob;
			this.mdM5 = MessageDigest.getInstance("MD5");
			this.mdSHA1 = MessageDigest.getInstance("SHA-1");
			this.afterWrite = afterWrite;
		}

		@Override
		public void writeTo(BufferedSink sink) throws IOException {
			InputStream src = blob.getPayload().openStream();
			src = mdM5 != null ? new DigestInputStream(src, mdM5) : src;
			src = mdSHA1 != null ? new DigestInputStream(src, mdSHA1) : src;

			try (Source source = Okio.source(src)) {
				sink.writeAll(source);
			}
			afterWrite.accept(new Hashes() {

				@Override
				public String md5() {
					return base16().encode(mdM5.digest());
				}

				@Override
				public String sha1() {
					return base16().encode(mdSHA1.digest());
				}

				@Override
				public String toString() {
					return "MD5: " + md5() + " SHA-1: " + sha1();
				}
			});
		}
	}

	private static final byte[] EMPTY_CONTENT = new byte[0];
	@SuppressWarnings("deprecation")
	private static final byte[] DIRECTORY_MD5 = Hashing.md5().hashBytes(EMPTY_CONTENT).asBytes();

	private static final Type GSON_MAP_TYPE = new TypeToken<Map<String, String>>() {
	}.getType();

	private static final Logger LOGGER = LoggerFactory.getLogger(HashingStrategyImpl.class);

	private static final String SEPARATOR = "/";

	private final ApiClient apiClient;

	private final String baseDirectory;

	private final boolean active;

	private final String hashFolder;

	private final RemoteFolder hashRemoteFolder;

	private final Gson gson;

	@Inject
	public HashingStrategyImpl(//
			ApiClient apiClient, //
			@Named(PCloudConstants.PROPERTY_BASEDIR) String baseDir, //
			@Named(PCloudConstants.PROPERTY_CUSTOM_HASH_ACTIVE) boolean active, //
			@Named(PCloudConstants.PROPERTY_CUSTOM_HASH_FOLDER) String hashFolder //
	) {
		this.baseDirectory = checkNotNull(baseDir, "Property " + PCloudConstants.PROPERTY_BASEDIR);
		this.active = active;
		this.apiClient = checkNotNull(apiClient, "PCloud api client");
		final String hashFolderName = checkNotNull(hashFolder,
				"Property " + PCloudConstants.PROPERTY_USERMETADATA_FOLDER);
		this.hashFolder = this.baseDirectory + SEPARATOR + hashFolderName;
		this.hashRemoteFolder = this.active ? PCloudUtils.createBaseDirectory(this.apiClient, this.hashFolder)
				: null;
		this.gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

		LOGGER.debug("User defined hash codes stored at '{}'", hashRemoteFolder);
	}

	/**
	 * Builds the hash file name for a given container and blob name.
	 * private static final Type GSON_MAP_TYPE = new TypeToken<Map<String,
	 * String>>() {
	 * }.getType();
	 * 
	 * @return Name of the file.
	 */
	private String buildMetadataFileName(String container, String key) {
		return base16().lowerCase()
				.encode(Hashing.sha256().hashString(container + SEPARATOR + key, StandardCharsets.UTF_8).asBytes())
				+ ".metadata.json";
	}

	/**
	 * Full file path for the hash file for a given container and blob name.
	 * 
	 * @param container Name of the container
	 * @param key       Key of the blob
	 * @return
	 */
	private String buildFilePath(String container, String key) {
		return hashFolder + SEPARATOR + buildMetadataFileName(container, key);
	}

	/**
	 * Full file path for the hash file for a given container and blob name.
	 * 
	 * @param container Name of the container
	 * @param key       Key of the blob
	 * @return
	 *         Returns local instance of PCloud {@link ApiClient}.
	 * 
	 * @return {@link ApiClient}
	 */
	private ApiClient getApiClient() {
		return this.apiClient;
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
	public CompletableFuture<Hashes> get(Blob blob) {
		if (this.active) {
			String container = blob.getMetadata().getContainer();
			String name = blob.getMetadata().getName();
			final String path = buildFilePath(container, name);

			return PCloudUtils.execute(this.getApiClient().loadFile(path)) //
					.thenApplyAsync(this::readHashes) //
					.thenApply(v -> {
						LOGGER.debug("Successfully loaded custom hashcodes for blob (hash file {}) {}{}{}: {}", path,
								container, SEPARATOR, name, v);
						return v;
					}) //
					.exceptionally(e -> this.notFoundDefault(e, () -> null))
					// if there is hash file found, load the file content, and create a new hash
					// code file
					.thenCompose(
							h -> h != null ? CompletableFuture.completedFuture(h) : loadFileAndComputeHashes(blob));

		} else {
			return CompletableFuture.completedFuture(new Hashes() {

				@Override
				public String md5() {
					return null;
				}

				@Override
				public String sha1() {
					return null;
				}

			});
		}
	}

	/**
	 * Loads the file content and computes the {@link Hashes} objects.
	 * 
	 * @param blob
	 * @return
	 */
	private CompletableFuture<Hashes> loadFileAndComputeHashes(Blob blob) {
		String path = this.baseDirectory + SEPARATOR + blob.getMetadata().getContainer() + SEPARATOR
				+ blob.getMetadata().getName();
		final CompletableFuture<RemoteFile> remoteFileRequest = PCloudUtils
				.execute(this.getApiClient().loadFile(path));
		return remoteFileRequest.thenApply(rf -> {
			if (rf.isFolder()) {
				return new Hashes() {

					@Override
					public String md5() {
						return base16().encode(DIRECTORY_MD5);
					}

					@Override
					public String sha1() {
						return rf.hash();
					}
				};
			} else {
				try (HashingInputStream hin = new HashingInputStream(Hashing.md5(), new BufferedInputStream(rf.byteStream()))) {
					while (hin.read() != -1)
						;
					return new Hashes() {

						@Override
						public String md5() {
							return base16().encode(hin.hash().asBytes());
						}

						@Override
						public String sha1() {
							return rf.hash();
						}

					};
				} catch(Exception e) {
					throw new RuntimeException(e);
				}
			}
		}).exceptionally(e -> this.notFoundDefault(e, () -> null));
	}

	private Hashes readHashes(RemoteFile rf) {
		if (rf.size() == 0) {
			// File is empty -> no need to read it
			return null;
		}
		try (JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(rf.byteStream())))) {
			final Map<String, String> result = gson.fromJson(reader, GSON_MAP_TYPE);
			return new Hashes() {

				@Override
				public String md5() {
					return result.get(Hashes.PROPERTY_MD5);
				}

				@Override
				public String sha1() {
					return result.get(Hashes.PROPERTY_SHA1);
				}
			};
		} catch (IOException e1) {
			throw new RuntimeException(e1);
		}
	}

	@Override
	public CompletableFuture<Void> delete(String container, String key) {
		if (this.active) {
			final String path = buildFilePath(container, key);
			return PCloudUtils.execute(getApiClient().deleteFile(path)) //
					.exceptionally(e -> notFoundDefault(e, () -> true)) //
					.thenAccept(
							v -> LOGGER.debug(
									"Successfully deleted custom hashcode file for blob (hashcode file {}) {}{}{}",
									path, container, SEPARATOR, key));
		} else {
			return CompletableFuture.completedFuture(null);
		}
	}

	@Override
	public CompletableFuture<Void> copy(String fromContainer, String fromKey, String toContainer, String toKey) {
		// TODO implement

		return CompletableFuture.completedFuture(null);
	}

	private CompletableFuture<Void> storeHashes(String container, String key, Hashes hashes) {
		if (active && hashes != null) {
			final String name = buildMetadataFileName(container, key);

			return CompletableFuture.supplyAsync(() -> toDataSource(hashes)) //
					.thenComposeAsync(ds -> PCloudUtils.execute(this.getApiClient()
							.createFile(this.hashRemoteFolder, name, ds, UploadOptions.OVERRIDE_FILE)))
					.thenAccept(rf -> LOGGER.debug(
							"Successfully uploaded custom hashcode map for blob (hash code file {}) {}{}{}: {}", name,
							container, SEPARATOR, key, hashes));
		}
		return CompletableFuture.completedFuture(null);
	}

	/**
	 * Creates a {@link DataSource} containing a json representation of the given
	 * metadata
	 * 
	 * @param metadata
	 * @return
	 */
	private DataSource toDataSource(Hashes hashes) {
		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (Writer w = new OutputStreamWriter(bos, StandardCharsets.UTF_8)) {
			gson.toJson(hashes.toMap(), GSON_MAP_TYPE, w);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		return DataSource.create(bos.toByteArray());
	}

	/**
	 * Returns a {@link DataSource} for the given {@link Blob}, which generates
	 * calculates all necessary hashes during transfering the {@link Blob} data to
	 * the {@link DataSource} reader. Upon completion of the transfer, the generated
	 * {@link Hashes} are stored.
	 */
	@Override
	public DataSource storeHashForOnWrite(Blob blob) {
		if (active) {
			try {
				return new AugementedBlobDataSource(blob, hashes -> {
					try {
						this.storeHashes(blob.getMetadata().getContainer(), blob.getMetadata().getName(), hashes).get();
					} catch (InterruptedException | ExecutionException e) {
						throw new RuntimeIOException(e);
					}
				});
			} catch (NoSuchAlgorithmException e) {
				throw new RuntimeException(e);
			}
		} else {
			return new BlobDataSource(blob);
		}
	}

}
