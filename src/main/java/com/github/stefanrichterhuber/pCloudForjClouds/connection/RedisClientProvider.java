package com.github.stefanrichterhuber.pCloudForjClouds.connection;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.inject.Inject;
import javax.inject.Named;

import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.inject.Provider;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;

public class RedisClientProvider implements Provider<RedisConnection<String, String>> {

    private final String redisUrl;

    @Inject
    public RedisClientProvider( //
            @Named(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING) String redis //
    ) {
        this.redisUrl = checkNotNull(redis,
                "Property " + PCloudConstants.PROPERTY_REDIS_CONNECT_STRING + " must be set");
    }

    @Override
    public RedisConnection<String, String> get() {
        RedisClient redisClient = new RedisClient(
                RedisURI.create(this.redisUrl));

        RedisConnection<String, String> connection = redisClient.connect();
        return connection;
    }

}
