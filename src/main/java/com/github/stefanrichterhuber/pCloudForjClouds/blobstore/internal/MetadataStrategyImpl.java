package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.BaseEncoding.base16;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.BlobHashes;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.ExternalBlobMetadata;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.MetadataStrategy;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.PCloudBlobStore;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.RemoteFile;
import com.pcloud.sdk.RemoteFolder;
import com.pcloud.sdk.UploadOptions;

public class MetadataStrategyImpl implements MetadataStrategy {
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

    /**
     * Creates a PCloud path to the target object starting with the
     * {@link #baseDirectory}.
     * 
     * @param content Path parts
     * @return Path
     */
    private String createFilePath(String... content) {
        if (content != null && content.length > 0) {
            return this.baseDirectory + SEPARATOR
                    + Arrays.asList(content).stream().collect(Collectors.joining(SEPARATOR));
        } else {
            return this.baseDirectory;
        }
    }

    @Override
    public CompletableFuture<ExternalBlobMetadata> get(String container, String key) {
        if (this.active) {
            final String path = buildMetadataFilePath(container, key);
            return PCloudUtils.execute(this.getApiClient().loadFile(path)) //
                    .thenApplyAsync(this::readMetadata) //
                    // If the metadata file is not found, create an empty metadata content
                    // This triggers loading the hashes in the next block
                    .exceptionally(e -> PCloudUtils.notFileFoundDefault(e,
                            () -> new ExternalBlobMetadata(container, key, null, Collections.emptyMap())))
                    .thenComposeAsync(v -> {
                        // First check if there are hashes stored at all
                        if (v.hashes() == null || !v.hashes().isValid()) {
                            LOGGER.debug(
                                    "There is no checksum available for blob {}/{}: recalculate", container, key);
                            return this.getTargetFile(container, key)
                                    .thenComposeAsync(this::calculateChecksumFromRemoteFile)
                                    .thenApply(hashes -> new ExternalBlobMetadata(container, key, hashes,
                                            v.customMetadata()))
                                    // Save the corrected metadata
                                    .thenComposeAsync(bm -> this.put(container, key, bm).thenApply(n -> bm));
                        }

                        // Then check if stored hashes are still valid
                        return getTargetFile(container, key).thenComposeAsync(rf -> {
                            if (rf != null && rf.isFile()) {
                                if (v.hashes().buildin().equals(rf.hash())) {
                                    // stored hash still valid
                                    LOGGER.debug("Found metadata for file {}/{} and it is contains vaild checksums!",
                                            container,
                                            key);
                                    return CompletableFuture.completedFuture(v);
                                } else {
                                    LOGGER.warn(
                                            "There was a checksum available for the blob {}/{} but it is not valid anymore: recalculate",
                                            container, key);
                                    // stored hash no long valid -> recalculate
                                    return this.calculateChecksumFromRemoteFile(rf)
                                            .thenApply(hashes -> new ExternalBlobMetadata(container, key, hashes,
                                                    v.customMetadata()))
                                            // Save the corrected metadata
                                            .thenComposeAsync(bm -> this.put(container, key, bm).thenApply(n -> bm));
                                }
                            }
                            return CompletableFuture
                                    .completedFuture(new ExternalBlobMetadata(container, key,
                                            BlobHashes.empty().withBuildin(rf != null ? rf.hash() : null),
                                            v.customMetadata()));
                        });
                    })
                    // If something bad happens during the previous block, just return empty
                    // metadata.
                    .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> new ExternalBlobMetadata(container,
                            key, BlobHashes.empty(), Collections.emptyMap())));
        } else {
            return CompletableFuture.completedFuture(EMPTY_METADATA);
        }
    }

    /**
     * Returns a {@link CompletableFuture} of the actual target file for the
     * metadata
     * 
     * @param container Container of the blob
     * @param key       Key of the blob
     * @return {@link RemoteFile}
     */
    protected CompletableFuture<RemoteFile> getTargetFile(String container, String key) {
        final String name = PCloudBlobStore.stripDirectorySuffix(key);
        final String path = this.createFilePath(container, name);
        final CompletableFuture<RemoteFile> remoteFileRequest = PCloudUtils
                .execute(this.getApiClient().loadFile(path));
        return remoteFileRequest;
    }

    /**
     * Completely loads the {@link RemoteFile} and calculates the necessary
     * checksums. This can be
     * a very expensive operation and should only be done, if the checksums are
     * missing.
     * 
     * @param rf {@link RemoteFile} of the blob
     * @return {@link BlobHashes} calculated
     */
    private CompletableFuture<BlobHashes> calculateChecksumFromRemoteFile(RemoteFile rf) {
        if (rf != null && rf.isFile()) {
            /*
             * Unfortunately apiClient.getChecksums() always returns null for md5 :/
             */

            return PCloudUtils.calculateChecksum(this.apiClient, rf.fileId()).thenCompose(blobHashes -> {
                // Check if the checksums delivers the required MD5 checksum (not available for
                // European accounts). If yes we are done.
                // Otherwise we have to download the file to calculate the checksums
                if (blobHashes.md5() != null) {
                    LOGGER.debug("pCloud checksum delivered the required MD5 checksums");
                    return CompletableFuture.completedFuture(blobHashes.withBuildin(rf.hash()));
                } else {
                    return CompletableFuture.supplyAsync(() -> {
                        LOGGER.warn(
                                "pCloud checksum does not give the required MD5 checksum. We have to download the file and calculate checksums ourselves :/");
                        try {
                            return BlobHashes.from(rf);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            });
        } else {
            return CompletableFuture.completedFuture(BlobHashes.empty().withBuildin(rf != null ? rf.hash() : null));
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
    private ExternalBlobMetadata readMetadata(RemoteFile rf) {
        if (rf.size() == 0) {
            // File is empty -> no need to read it
            return EMPTY_METADATA;
        }
        try (JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(rf.byteStream())))) {
            final ExternalBlobMetadata result = gson.fromJson(reader, ExternalBlobMetadata.class);
            return result;
        } catch (IOException e1) {
            throw new RuntimeException(e1);
        }
    }

    @Override
    public CompletableFuture<Void> put(String container, String key, ExternalBlobMetadata metadata) {
        if (this.active) {
            if (metadata == null) {
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
            if (metadata != null) {
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
    private DataSource writeMetadata(ExternalBlobMetadata metadata) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (Writer w = new OutputStreamWriter(bos, StandardCharsets.UTF_8)) {
            gson.toJson(metadata, ExternalBlobMetadata.class, w);
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
                    .exceptionally(e -> PCloudUtils.notFileFoundDefault(e, () -> true)) //
                    .thenAccept(
                            v -> LOGGER.debug("Successfully deleted custom metadata for blob (metadata file {}) {}{}{}",
                                    path, container, SEPARATOR, key));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

}
