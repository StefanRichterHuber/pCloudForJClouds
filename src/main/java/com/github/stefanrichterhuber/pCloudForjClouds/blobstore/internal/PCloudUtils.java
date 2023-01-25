package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.BlobHashes;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.Call;
import com.pcloud.sdk.Callback;
import com.pcloud.sdk.RemoteEntry;
import com.pcloud.sdk.RemoteFolder;

import okhttp3.HttpUrl;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;

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
     * Utility function to execute a {okhttp3.Call} async and wrap its results into
     * a {@link CompletableFuture}.
     * 
     * @param call {okhttp3.Call} to execute
     * @return {@link CompletableFuture} containing the {@link Response}
     */
    public static CompletableFuture<Response> execute(okhttp3.Call call) {
        final CompletableFuture<Response> result = new CompletableFuture<Response>();
        call.enqueue(new okhttp3.Callback() {

            @Override
            public void onResponse(okhttp3.Call call, Response response) throws IOException {
                result.complete(response);
            }

            @Override
            public void onFailure(okhttp3.Call call, IOException e) {
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    /**
     * Creates an {@link OkHttpClient} from the given {@link ApiClient} inheriting
     * the authentication.
     * 
     * @param apiClient
     * @return
     */
    public static OkHttpClient getHTTPClient(ApiClient apiClient) {
        OkHttpClient.Builder httpClientBuilder = new OkHttpClient.Builder()
//              .readTimeout(builder.readTimeoutMs(), TimeUnit.MILLISECONDS)
//              .writeTimeout(builder.writeTimeoutMs(), TimeUnit.MILLISECONDS)
//              .connectTimeout(builder.connectTimeoutMs(), TimeUnit.MILLISECONDS)
                .protocols(Collections.singletonList(Protocol.HTTP_1_1));
        // .addInterceptor(new GlobalRequestInterceptor(userAgent, globalParams));

        httpClientBuilder.addInterceptor((Interceptor) apiClient.authenticator());
        OkHttpClient httpClient = httpClientBuilder.build();
        return httpClient;
    }

    /**
     * Calculates the checksums of a whole file. Calculated checksums depends on the
     * location of the pCloud store. Unfortunately no MD5 for European customers :/
     * 
     * @see https://docs.pcloud.com/methods/file/checksumfile.html
     * @param apiClient {@link ApiClient} to access the pCloud backend
     * @param filePath  Path fo the file
     * @return Checksums calculated
     */
    public static CompletableFuture<BlobHashes> calculateChecksum(ApiClient apiClient, String filePath) {
        var apiHost = HttpUrl.parse("https://" + apiClient.apiHost());
        var httpClient = PCloudUtils.getHTTPClient(apiClient);

        var gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

        final HttpUrl.Builder urlBuilder = apiHost.newBuilder().addPathSegment("checksumfile") //
                .addQueryParameter("path", filePath) //
        ;

        Request request = new Request.Builder().url(apiHost).url(urlBuilder.build()).get().build();

        return execute(httpClient.newCall(request)).thenApply(resp -> {
            try (Response response = resp) {
                JsonReader reader = new JsonReader(
                        new BufferedReader(new InputStreamReader(response.body().byteStream())));
                BlobHashes result = gson.fromJson(reader, BlobHashes.class);
                return result;
            }
        });
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
