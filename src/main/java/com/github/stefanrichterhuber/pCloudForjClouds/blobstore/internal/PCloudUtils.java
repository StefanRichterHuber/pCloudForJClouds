package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.BlobHashes;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.common.base.Optional;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.Authenticators;
import com.pcloud.sdk.Call;
import com.pcloud.sdk.Callback;
import com.pcloud.sdk.PCloudSdk;
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
     * Converts a
     * {@link Collection} of {@link CompletableFuture}s to a
     * {@link CompletableFuture} of a {@link Collection} using
     * {@link CompletableFuture#allOf(CompletableFuture...)}
     * 
     * @param <T>
     * @param jobs {@link Collection} of {@link CompletableFuture}s to convert
     * @return
     */
    public static <T, C extends Collection<T>> CompletableFuture<C> allOf(final Collection<CompletableFuture<T>> jobs,
            final Supplier<C> collectionFactory) {
        return CompletableFuture.allOf(jobs.toArray(new CompletableFuture[jobs.size()]))
                .thenApply(v -> jobs.stream().map(CompletableFuture::join)
                        .collect(Collectors.toCollection(collectionFactory)));
    }

    /**
     * Converts a
     * {@link Collection} of {@link CompletableFuture}s to a
     * {@link CompletableFuture} of a {@link List} using
     * {@link CompletableFuture#allOf(CompletableFuture...)}
     * 
     * @param <T>
     * @param jobs {@link Collection} of {@link CompletableFuture}s to convert
     * @return
     */
    public static <T> CompletableFuture<List<T>> allOf(final Collection<CompletableFuture<T>> jobs) {
        return allOf(jobs, ArrayList::new);
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

        final HttpUrl.Builder urlBuilder = apiHost.newBuilder() //
                .addPathSegment("checksumfile") //
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
     * Calculates the checksums of a whole file. Calculated checksums depends on the
     * location of the pCloud store. Unfortunately no MD5 for European customers :/
     * 
     * @see https://docs.pcloud.com/methods/file/checksumfile.html
     * @param apiClient {@link ApiClient} to access the pCloud backend
     * @param fileId    ID of the file
     * @return Checksums calculated
     */
    public static CompletableFuture<BlobHashes> calculateChecksum(ApiClient apiClient, long fileId) {
        var apiHost = HttpUrl.parse("https://" + apiClient.apiHost());
        var httpClient = PCloudUtils.getHTTPClient(apiClient);

        var gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

        final HttpUrl.Builder urlBuilder = apiHost.newBuilder() //
                .addPathSegment("checksumfile") //
                .addQueryParameter("fileid", Long.toString(fileId)) //
        ;

        Request request = new Request.Builder().url(urlBuilder.build()).get().build();

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
     * This method returns closest API server to the requesting client. The biggest
     * speed gain will be with upload methods. Clients should have fallback logic.
     * If request to API server different from api.pcloud.com fails (network error)
     * the client should fallback to using api.pcloud.com.
     * 
     * @param apiEndpoint either eapi.pcloud.com or api.pcloud.com
     * 
     * @return
     */
    public static CompletableFuture<GetApiResponse> getApiServer(String apiEndpoint) {
        final OkHttpClient httpClient = new OkHttpClient.Builder()
                .protocols(Collections.singletonList(Protocol.HTTP_1_1)).build();

        var gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();

        final HttpUrl.Builder urlBuilder = HttpUrl.parse("https://" + apiEndpoint).newBuilder() //
                .addPathSegment("getapiserver") //
        ;

        Request request = new Request.Builder().url(urlBuilder.build()).get().build();

        return execute(httpClient.newCall(request)).thenApply(resp -> {
            try (Response response = resp) {
                JsonReader reader = new JsonReader(
                        new BufferedReader(new InputStreamReader(response.body().byteStream())));
                GetApiResponse result = gson.fromJson(reader, GetApiResponse.class);
                return result;
            }
        });
    }

    /**
     * List updates of the user's folders/files.
     * 
     * Optionally, takes the parameter diffid, which if provided returns only
     * changes since that diffid.
     * 
     * @param apiClient {@link ApiClient} to connect to backend
     * @param diffId    Optional, receive only changes since that diffId
     * @param limit     Optional, receive only that many events
     * @param block     Block until an event arrives
     * @return
     */
    public static CompletableFuture<DiffResponse> getDiff(ApiClient apiClient, Integer diffId, Integer limit,
            boolean block) {
        var apiHost = HttpUrl.parse("https://" + apiClient.apiHost());
        var httpClient = PCloudUtils.getHTTPClient(apiClient);

        var gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation()
                .setDateFormat("EEE, dd MMM yyyy HH:mm:ss zzzz").create();

        HttpUrl.Builder urlBuilder = apiHost.newBuilder() //
                .addPathSegment("diff") //
        ;
        if (diffId != null) {
            urlBuilder = urlBuilder.addQueryParameter("diffid", Integer.toString(diffId));
        }
        if (limit != null) {
            urlBuilder = urlBuilder.addQueryParameter("limit", Integer.toString(limit));
        }
        if (block) {
            urlBuilder = urlBuilder.addQueryParameter("block", "1");
        }

        Request request = new Request.Builder().url(urlBuilder.build()).get().build();

        return execute(httpClient.newCall(request)).thenApply(resp -> {
            try (Response response = resp) {
                JsonReader reader = new JsonReader(
                        new BufferedReader(new InputStreamReader(response.body().byteStream())));
                DiffResponse result = gson.fromJson(reader, DiffResponse.class);
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
                LOGGER.debug("Required folder '{}' already exists", folder);
                return remoteFolder;
            }
        } catch (ApiError e) {
            if (PCloudError.isEntryNotFound(e)) {
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

    /**
     * Utility method for exception handling, returning a default value if the
     * requested file was not found. If the error was not a file not found error, a
     * {@link Throwable} is thrown.
     * 
     * @param <T>
     * @param th               {@link Throwable} to handle
     * @param defaultValue     {@link Supplier} for the default value if the file
     *                         was
     *                         not found
     * @param exceptionFactory {@link Function} to wrap the {@link Exception} into a
     *                         {@link Throwable}.
     * @return Default value, or throws a {@link Throwable}.
     */
    public static <Th extends Throwable, T, E extends Throwable> T notFileFoundDefault(Th th,
            Supplier<T> defaultValue,
            Function<Th, E> exceptionFactory) throws E {
        if (th instanceof ApiError && PCloudError.isEntryNotFound((ApiError) th)) {
            return defaultValue.get();
        }
        if (th != null && th.getCause() instanceof ApiError && PCloudError.isEntryNotFound((ApiError) th.getCause())) {
            return defaultValue.get();
        }
        if (th != null && th.getCause() != null && th.getCause().getCause() instanceof ApiError
                && PCloudError.isEntryNotFound((ApiError) th.getCause().getCause())) {
            return defaultValue.get();
        }
        throw exceptionFactory.apply(th);
    }

    /**
     * Utility method for exception handling, returning a default value if the
     * requested file was not found. If the error was not a file not found error, a
     * {@link RuntimeException} is thrown.
     * 
     * @param <T>
     * @param e            {@link Throwable} to handle
     * @param defaultValue {@link Supplier} for the default value if the file
     *                     was
     *                     not found
     * @return Default value, or throws a {@link Throwable}.
     */
    public static <Th extends Throwable, T> T notFileFoundDefault(Th e, Supplier<T> defaultValue)
            throws RuntimeException {
        return notFileFoundDefault(e, defaultValue,
                ex -> ex instanceof RuntimeException ? (RuntimeException) ex : new RuntimeException(ex));
    }

    /**
     * There are different API endpoints for different accounts (see
     * {@link PCloudConstants#PROPERTY_PCLOUD_API_VALUES}. Test all possible
     * endpoints for the correct one.
     * 
     * @param id Oauth id of the user
     * @return API Endpoint found.
     */
    public static Optional<String> testForAPIEndpoint(String id) {
        for (String pCloudHost : PCloudConstants.PROPERTY_PCLOUD_API_VALUES) {
            ApiClient apiClient = PCloudSdk.newClientBuilder().apiHost(pCloudHost)
                    .authenticator(Authenticators.newOAuthAuthenticator(id)).create();
            try {
                RemoteFolder remoteFolder = apiClient.listFolder("/").execute();
                if (remoteFolder != null) {
                    return Optional.of(pCloudHost);
                }
            } catch (IOException | ApiError e) {
                // Ignore this is is possible the wrong client
            }

            // Also check for the closest API endpoint
        }
        return Optional.absent();
    }
}
