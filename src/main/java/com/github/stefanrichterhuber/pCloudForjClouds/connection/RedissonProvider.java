package com.github.stefanrichterhuber.pCloudForjClouds.connection;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.inject.Inject;
import javax.inject.Named;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.TransportMode;

import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.inject.Provider;

public class RedissonProvider implements Provider<RedissonClient> {

    private final String redisUrl;

    @Inject
    public RedissonProvider( //
            @Named(PCloudConstants.PROPERTY_REDIS_CONNECT_STRING) String redis //
    ) {
        this.redisUrl = checkNotNull(redis,
                "Property " + PCloudConstants.PROPERTY_REDIS_CONNECT_STRING + " must be set");
    }

    @Override
    public RedissonClient get() {

        Config config = new Config();
        config.setTransportMode(TransportMode.NIO);
        config.useSingleServer().setAddress(redisUrl);
        RedissonClient redisson = Redisson.create(config);
        return redisson;
    }

}
