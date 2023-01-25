package com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.internal;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import javax.annotation.Resource;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.jclouds.logging.Logger;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudUtils;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileOps;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.stream.JsonReader;
import com.pcloud.sdk.ApiClient;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

@Singleton
public class PCloudFileOpsImpl implements PCloudFileOps {
    @Resource
    protected Logger logger = Logger.NULL;

    private final HttpUrl apiHost;
    private final OkHttpClient httpClient;
    private final Gson gson;

    @Inject
    public PCloudFileOpsImpl(ApiClient apiClient) {
        this.apiHost = HttpUrl.parse("https://" + apiClient.apiHost());
        this.httpClient = PCloudUtils.getHTTPClient(apiClient);

        this.gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    }

    @Override
    public FileDescriptorImpl open(long parentFolder, String fileName, Flag... flags) throws IOException {
        int flag = flags != null ? Arrays.asList(flags).stream().filter(f -> f != null).mapToInt(Flag::getValue).sum()
                : Flag.CREATE.getValue();
        final HttpUrl.Builder urlBuilder = apiHost.newBuilder().addPathSegment("file_open") //
                .addQueryParameter("folderid", String.valueOf(parentFolder)) //
                .addQueryParameter("flags", String.valueOf(flag)) //
                .addQueryParameter("name", fileName);

        Request request = new Request.Builder().url(apiHost).url(urlBuilder.build()).get().build();
        try (Response response = httpClient.newCall(request).execute()) {
            JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(response.body().byteStream())));
            FileDescriptorImpl result = gson.fromJson(reader, FileDescriptorImpl.class);
            result.setFileOps(this);

            if (result.result() == 0) {
                return result;
            } else {
                throw new IOException("Writing content leads to result: " + result.result());
            }
        }
    }

    public FileDescriptorImpl open(long fileid, Flag... flags) throws IOException {
        final int flag = flags != null
                ? Arrays.asList(flags).stream().filter(f -> f != null).mapToInt(Flag::getValue).sum()
                : Flag.CREATE.getValue();
        final HttpUrl.Builder urlBuilder = apiHost.newBuilder().addPathSegment("file_open") //
                .addQueryParameter("fileid", String.valueOf(fileid)) //
                .addQueryParameter("flags", String.valueOf(flag)); //

        final Request request = new Request.Builder().url(apiHost).url(urlBuilder.build()).get().build();
        try (Response response = httpClient.newCall(request).execute()) {
            JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(response.body().byteStream())));
            FileDescriptorImpl result = gson.fromJson(reader, FileDescriptorImpl.class);
            result.setFileOps(this);

            if (result.result() == 0) {
                return result;
            } else {
                throw new IOException("Writing content leads to result: " + result.result());
            }
        }
    }

    /**
     * Returns the current file size
     * 
     * @param fd File descriptor
     * @return File size
     * @throws IOException
     */
    public long getSize(int fd) throws IOException {
        final HttpUrl.Builder urlBuilder = apiHost.newBuilder() //
                .addPathSegment("file_size") //
                .addQueryParameter("fd", String.valueOf(fd));

        final Request request = new Request.Builder().url(apiHost).url(urlBuilder.build()).get().build();

        try (Response response = httpClient.newCall(request).execute()) {
            JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(response.body().byteStream())));
            FileSizeResultImpl result = gson.fromJson(reader, FileSizeResultImpl.class);
            return result.size();
        }
    }

    /**
     * Calculates both md5 and sha1 checksums for the file
     * 
     * @param fd     File descriptor
     * @param offset Offset
     * @param count  Count
     * @return Checksum calculated
     * @throws IOException
     */
    public FileChecksumResultImpl calculateChecksum(int fd, long offset, long count) throws IOException {
        HttpUrl.Builder urlBuilder = apiHost.newBuilder() //
                .addPathSegment("file_checksum") //
                .addQueryParameter("fd", String.valueOf(fd)).addQueryParameter("count", String.valueOf(count))
                .addQueryParameter("offset", String.valueOf(offset));

        final Request request = new Request.Builder().url(apiHost).url(urlBuilder.build()).get().build();

        try (Response response = httpClient.newCall(request).execute()) {
            JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(response.body().byteStream())));
            FileChecksumResultImpl result = gson.fromJson(reader, FileChecksumResultImpl.class);
            return result;
        }
    }

    /**
     * Locks or unlocks the file
     * 
     * @param fd      File descriptor
     * @param type    Lock type (0 unlock, 1 shared lock, 2 exclusive lock)
     * @param noblock Wait for the block
     * @return Is the object locked?
     * @throws IOException
     */
    public boolean lock(int fd, int type, boolean noblock) throws IOException {
        HttpUrl.Builder urlBuilder = apiHost.newBuilder() //
                .addPathSegment("file_lock") //
                .addQueryParameter("type", String.valueOf(type)) //
                .addQueryParameter("fd", String.valueOf(fd));

        if (noblock) {
            urlBuilder = urlBuilder.addQueryParameter("noblock", "true");
        }
        final Request request = new Request.Builder().url(apiHost).url(urlBuilder.build()).get().build();

        try (Response response = httpClient.newCall(request).execute()) {
            JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(response.body().byteStream())));
            LockResultImpl result = gson.fromJson(reader, LockResultImpl.class);

            if (type == 0) {
                if (result.locked()) {
                    logger.info("Failed to unlock file %s", fd);
                } else {
                    logger.info("Successfully unlocked file %s", fd);
                }
            } else {
                if (result.locked()) {
                    logger.info("Successfully locked file %s", fd);
                } else {
                    logger.info("Failed to lock file %s", fd);
                }
            }
            return result.locked();
        }
    }

    /**
     * Closes the open file
     * 
     * @param fd File descriptor
     * @return
     * @throws IOException
     */
    public boolean close(int fd) throws IOException {
        final HttpUrl.Builder urlBuilder = apiHost.newBuilder() //
                .addPathSegment("file_close") //
                .addQueryParameter("fd", String.valueOf(fd));
        final Request request = new Request.Builder().url(apiHost).url(urlBuilder.build()).get().build();
        try (Response response = httpClient.newCall(request).execute()) {
            JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(response.body().byteStream())));
            FileOpsResultImpl result = gson.fromJson(reader, FileOpsResultImpl.class);
            return result.result() == 0;
        }
    }

    /**
     * Writes content to the open file
     * 
     * @param fd      File descriptor
     * @param content Content to write
     * @return
     * @throws IOException
     */
    public long write(int fd, byte[] content) throws IOException {
        final HttpUrl.Builder urlBuilder = apiHost.newBuilder() //
                .addPathSegment("file_write") //
                .addQueryParameter("fd", String.valueOf(fd));

        final RequestBody requestBody = RequestBody.create(MediaType.parse("application/octet-stream"), content);

        final Request request = new Request.Builder().url(apiHost).url(urlBuilder.build()).put(requestBody).build();
        try (Response response = httpClient.newCall(request).execute()) {
            JsonReader reader = new JsonReader(new BufferedReader(new InputStreamReader(response.body().byteStream())));
            FileWriteResultImpl result = gson.fromJson(reader, FileWriteResultImpl.class);
            if (result.result() == 0) {
                return result.bytes();
            } else {
                throw new IOException("Writing content leads to result: " + result.result());
            }
        }
    }

}
