package com.github.stefanrichterhuber.pCloudForjClouds.reference;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.jclouds.Constants;

public class PCloudConstants {
    /**
     * Prefix common for all properties
     */
    public static final String COMMON_PREFIX = "jclouds.pcloud.";

    /**
     * Specify the base directory where provider starts its file operations - must
     * exists
     */
    public static final String PROPERTY_BASEDIR = COMMON_PREFIX + "basedir";

    /**
     * Client secret to authenticate user
     */
    public static final String PROPERTY_CLIENT_SECRET = COMMON_PREFIX + "clientsecret";

    /**
     * Possible values for the property {@link Constants#PROPERTY_ENDPOINT}
     */
    public static final List<String> PROPERTY_PCLOUD_API_VALUES = Arrays.asList("api.pcloud.com", "eapi.pcloud.com");

    /**
     * Redis backend for metadata storage
     */
    public static final String PROPERTY_REDIS_CONNECT_STRING = COMMON_PREFIX + "redis";

    /**
     * Is support for user-defined metadata active?
     */
    public static final String PROPERTY_USERMETADATA_ACTIVE = COMMON_PREFIX + "usermetadata.active";

    /**
     * Folder within the {@link #PROPERTY_BASEDIR} to store the user defined
     * metatdata
     */
    public static final String PROPERTY_USERMETADATA_FOLDER = COMMON_PREFIX + "usermetadata.folder";

    /**
     * Interval in minutes the metadata folder is checked for orphaned entries
     * (metadata entries without corresponding file / folder)
     * if negative no scan is started. If 0, only one scan is started.
     */
    public static final String PROPERTY_SANITIZE_METADATA_INTERVAL_MIN = COMMON_PREFIX
            + "usermetadata.sanitize.interval";

    /**
     * Interval in minutes the content of the metadatafolder is synchronized with
     * the local cache.
     * if negative no sync occours. If 0, only one sync at startup is executed.
     */
    public static final String PROPERTY_SYNCHRONIZE_METADATA_INTERVAL_MIN = COMMON_PREFIX
            + "usermetadata.synchronize.interval";

    /**
     * Load all possible values from environment variables.
     * 
     * @return
     */
    public static Properties fromEnv() {
        Properties result = new Properties();
        // All our properties start with jclouds.pcloud. -> so search for JC
        String envPrefix = PCloudConstants.COMMON_PREFIX.toUpperCase().replace('.', '_');
        for (var entry : System.getenv().entrySet()) {
            if (entry.getKey().startsWith(envPrefix)) {
                final String propertyName = entry.getKey().toLowerCase().replace('_', '.');
                final String propertyValue = entry.getValue();
                result.put(propertyName, propertyValue);
            }
        }
        return result;
    }

    private PCloudConstants() {
        throw new AssertionError("intentionally unimplemented");
    }
}
