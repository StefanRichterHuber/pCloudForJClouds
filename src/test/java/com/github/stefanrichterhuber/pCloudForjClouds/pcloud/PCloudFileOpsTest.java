package com.github.stefanrichterhuber.pCloudForjClouds.pcloud;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.stefanrichterhuber.pCloudForjClouds.connection.PCloudApiClientProvider;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileOps;
import com.github.stefanrichterhuber.pCloudForjClouds.connection.fileops.PCloudFileOps.Flag;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.ApiError;
import com.pcloud.sdk.DataSource;
import com.pcloud.sdk.RemoteFile;

public class PCloudFileOpsTest {
    public static final String TEST_FOLDER = "/test-folder-" + UUID.randomUUID().toString();

    private ApiClient apiClient;
    private PCloudFileOps fileOps;

    @Before
    public void setup() throws IOException, ApiError {
        final String token = System.getenv("PCLOUD_TOKEN");

        apiClient = PCloudApiClientProvider.create(token);
        fileOps = PCloudFileOps.create(apiClient);

        apiClient.createFolder(TEST_FOLDER).execute();

    }

    @After
    public void teardown() throws IOException, ApiError {
        apiClient.deleteFolder(TEST_FOLDER, true).execute();
    }

    @Test
    public void shouldReadFromInputStreamTest() throws IOException, ApiError {
        List<String> content = Arrays.asList("O rose, thou art sick!\r\n", "The invisible worm,\r\n",
                "That flies in the night,\r\n", "In the howling storm.\r\n",
                "Has found out thy bed\r\n",
                "Of crimson joy,\r\n", "And his dark secret love\r\n", "Does thy life destroy.");
        String mergedContent = content.stream().collect(Collectors.joining());

        RemoteFile remoteFile = this.apiClient.createFile(TEST_FOLDER, "testfile1.txt",
                DataSource.create(mergedContent.getBytes(StandardCharsets.UTF_8))).execute();

        // Read content line-by-ine
        try (InputStream is = fileOps.open(remoteFile.fileId(), Flag.APPEND).inputStream()) {
            for (String c : content) {
                final int len = c.getBytes(StandardCharsets.UTF_8).length;
                final byte[] result = is.readNBytes(len);
                assertEquals(len, result.length);
                assertEquals(c, new String(result, StandardCharsets.UTF_8));
            }
        }

        // Read content at once
        try (InputStream is = fileOps.open(remoteFile.fileId(), Flag.APPEND).inputStream()) {
            final byte[] result = is.readAllBytes();
            assertEquals(mergedContent, new String(result, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void shouldSupportSkipTest() throws IOException, ApiError {
        List<String> content = Arrays.asList("O rose, thou art sick!\r\n", "The invisible worm,\r\n",
                "That flies in the night,\r\n", "In the howling storm.\r\n",
                "Has found out thy bed\r\n",
                "Of crimson joy,\r\n", "And his dark secret love\r\n", "Does thy life destroy.");
        String mergedContent = content.stream().collect(Collectors.joining());

        RemoteFile remoteFile = this.apiClient.createFile(TEST_FOLDER, "testfile2.txt",
                DataSource.create(mergedContent.getBytes(StandardCharsets.UTF_8))).execute();

        // Read content line-by-ine
        try (InputStream is = fileOps.open(remoteFile.fileId(), Flag.APPEND).inputStream()) {
            // Ignore first line
            is.skip(content.get(0).getBytes(StandardCharsets.UTF_8).length);

            // Read second line the first time
            {
                String c = content.get(1);
                final int len = c.getBytes(StandardCharsets.UTF_8).length;
                final byte[] result = is.readNBytes(len);
                assertEquals(len, result.length);
                assertEquals(c, new String(result, StandardCharsets.UTF_8));
            }
        }
    }

    @Test
    public void shouldSupportMarkTest() throws IOException, ApiError {
        List<String> content = Arrays.asList("O rose, thou art sick!\r\n", "The invisible worm,\r\n",
                "That flies in the night,\r\n", "In the howling storm.\r\n",
                "Has found out thy bed\r\n",
                "Of crimson joy,\r\n", "And his dark secret love\r\n", "Does thy life destroy.");
        String mergedContent = content.stream().collect(Collectors.joining());

        RemoteFile remoteFile = this.apiClient.createFile(TEST_FOLDER, "testfile3.txt",
                DataSource.create(mergedContent.getBytes(StandardCharsets.UTF_8))).execute();

        // Read content line-by-ine
        try (InputStream is = fileOps.open(remoteFile.fileId(), Flag.APPEND).inputStream()) {
            // ignore first line
            is.skip(content.get(0).getBytes(StandardCharsets.UTF_8).length);
            // Set mark
            is.mark(0);

            // Read second line the first time
            {
                String c = content.get(1);
                final int len = c.getBytes(StandardCharsets.UTF_8).length;
                final byte[] result = is.readNBytes(len);
                assertEquals(len, result.length);
                assertEquals(c, new String(result, StandardCharsets.UTF_8));
            }
            // Apply reset
            is.reset();
            // Read second line the second time
            {
                String c = content.get(1);
                final int len = c.getBytes(StandardCharsets.UTF_8).length;
                final byte[] result = is.readNBytes(len);
                assertEquals(len, result.length);
                assertEquals(c, new String(result, StandardCharsets.UTF_8));
            }

        }
    }

    @Test
    public void shouldWriteToStreamTest() throws IOException, ApiError {
        List<String> content = Arrays.asList("O rose, thou art sick!\r\n", "The invisible worm,\r\n",
                "That flies in the night,\r\n", "In the howling storm.\r\n",
                "Has found out thy bed\r\n",
                "Of crimson joy,\r\n", "And his dark secret love\r\n", "Does thy life destroy.");
        String mergedContent = content.stream().collect(Collectors.joining());

        RemoteFile remoteFile = this.apiClient.createFile(TEST_FOLDER, "testfile4.txt",
                DataSource.EMPTY).execute();

        // Write file
        try (OutputStream os = fileOps.open(remoteFile.fileId(), Flag.APPEND).openStream();
                OutputStreamWriter w = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
            w.append(mergedContent);
        }

        // Read back file
        try (InputStream is = fileOps.open(remoteFile.fileId(), Flag.APPEND).inputStream()) {
            final byte[] result = is.readAllBytes();
            assertEquals(mergedContent, new String(result, StandardCharsets.UTF_8));
        }

    }
}
