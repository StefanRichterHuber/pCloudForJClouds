package com.github.stefanrichterhuber.pCloudForjClouds.pcloud;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.RedisMetadataStrategyImpl;

public class RedisMetadataStrategyImplTest {
    private final static Logger LOGGER = LoggerFactory.getLogger(RedisMetadataStrategyImplTest.class);

    @Test
    public void shouldEncodeAndDecodeFiles() {
        final String a = RedisMetadataStrategyImpl.getMetadataFileName("abc", "xyz.txt");
        final String b = RedisMetadataStrategyImpl.getMetadataFileName("abc", "f1/f2/xyz.txt");
        final String c = RedisMetadataStrategyImpl.getMetadataFileName("abc", null);

        assertEquals("abc", RedisMetadataStrategyImpl.getContainerFromMetadataFileName(a));
        assertEquals("abc", RedisMetadataStrategyImpl.getContainerFromMetadataFileName(b));
        assertEquals("abc", RedisMetadataStrategyImpl.getContainerFromMetadataFileName(c));

        assertEquals("xyz.txt", RedisMetadataStrategyImpl.getKeyFromMetadataFileName(a));
        assertEquals("f1/f2/xyz.txt", RedisMetadataStrategyImpl.getKeyFromMetadataFileName(b));
        assertEquals(null, RedisMetadataStrategyImpl.getKeyFromMetadataFileName(c));

        LOGGER.info(a);
        LOGGER.info(b);
        LOGGER.info(c);
    }
}
