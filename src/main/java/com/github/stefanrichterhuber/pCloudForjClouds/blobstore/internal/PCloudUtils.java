package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.Call;
import com.pcloud.sdk.Callback;
import com.pcloud.sdk.RemoteEntry;
import com.pcloud.sdk.RemoteFolder;

public final class PCloudUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(PCloudUtils.class);

    private static final String SEPARATOR = "/";

    private PCloudUtils() {
        throw new AssertionError("intentionally unimplemented");
    }

    /**
     * Utility function to execute a {@link Call} async and wrap its results in a
     * {@link CompletableFuture}.
     * 
     * @param <T>
     * @param call {@link Call} to execute and wrap
     * @return {@link CompletableFuture} containing the result of the {@link Call}.
     */
    public static <T> CompletableFuture<T> execute(Call<T> call) {
        final CompletableFuture<T> result = new CompletableFuture<T>();

        call.enqueue(new Callback<T>() {

            @Override
            public void onResponse(Call<T> call, T response) {
                result.complete(response);
            }

            @Override
            public void onFailure(Call<T> call, Throwable t) {
                result.completeExceptionally(t);
            }
        });
        return result;
    }

    /**
     * Creates a folder and all necessary root folders
     */
    public static RemoteFolder createBaseDirectory(ApiClient apiClient, String folder) {
        if (folder == null || folder.equals("")) {
            folder = SEPARATOR;
        }
        try {
            RemoteFolder remoteFolder = apiClient.loadFolder(folder).execute();
            if (remoteFolder != null) {
                LOGGER.info("Required folder '{}' already exists", folder);
                return remoteFolder;
            }
        } catch (ApiError e) {
            final PCloudError pCloudError = PCloudError.parse(e);
            if (pCloudError == PCloudError.FILE_NOT_FOUND || pCloudError == PCloudError.FILE_OR_FOLDER_NOT_FOUND
                    || pCloudError == PCloudError.DIRECTORY_DOES_NOT_EXIST) {
                // Folder simply not found, continue trying the parent folder
            } else {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final String parentDir = folder.substring(0, folder.lastIndexOf(SEPARATOR));
        final RemoteFolder parentFolder = createBaseDirectory(apiClient, parentDir);

        // Parent folders are all created -> now create folder itself
        final String name = folder.substring(folder.lastIndexOf(SEPARATOR) + 1);
        try {
            final RemoteFolder rf = apiClient.createFolder(parentFolder.folderId(), name).execute();
            if (rf != null) {
                LOGGER.info("Required folder {} with id {} created!", folder, rf.folderId());
            }
            return rf;
        } catch (ApiError e) {
            final PCloudError pCloudError = PCloudError.parse(e);
            if (pCloudError == PCloudError.ALREADY_EXISTS) {
                // File already exists, try to fetch it from the parent folder
                try {
                    RemoteFolder remoteFolder = apiClient.loadFolder(parentFolder.folderId()).execute().children()
                            .stream()
                            .filter(RemoteEntry::isFolder).map(RemoteEntry::asFolder)
                            .filter(rf -> rf.name().equals(name)).findFirst().orElse(null);
                    return remoteFolder;
                } catch (IOException | ApiError e1) {
                    // Ignore --> just throw the previous exception
                }
            }
            throw new PCloudBlobStoreException(e);
        } catch (IOException e) {
            throw new PCloudBlobStoreException(e);
        }
    }
}
