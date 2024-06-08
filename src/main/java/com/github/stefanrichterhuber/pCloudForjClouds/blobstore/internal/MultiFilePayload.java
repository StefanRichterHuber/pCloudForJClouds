package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.List;

import org.jclouds.io.MutableContentMetadata;
import org.jclouds.io.payloads.BasePayload;

public class MultiFilePayload extends BasePayload<List<File>> {

    public MultiFilePayload(List<File> content, MutableContentMetadata contentMetadata) {
        super(content, contentMetadata);
    }

    public MultiFilePayload(List<File> content) {
        super(content);
    }

    @Override
    public InputStream openStream() throws IOException {
        final InputStream contentStream = getRawContent().stream().map(f -> {
            try {
                return (InputStream) new FileInputStream(f);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }).reduce(InputStream.nullInputStream(),
                (i1, i2) -> new SequenceInputStream(i1, i2));

        return contentStream;
    }

    public void deleteFiles() {
        getRawContent().stream().forEach(File::delete);
    }
}
