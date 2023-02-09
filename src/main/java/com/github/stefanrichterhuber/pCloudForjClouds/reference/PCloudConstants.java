package com.github.stefanrichterhuber.pCloudForjClouds.reference;

import java.util.Arrays;
import java.util.List;

import org.jclouds.Constants;

public class PCloudConstants {
    /**
     * Specify the base directory where provider starts its file operations - must
     * exists
     */
    public static final String PROPERTY_BASEDIR = "jclouds.pcloud.basedir";

    /**
     * Client secret to authenticate user
     */
    public static final String PROPERTY_CLIENT_SECRET = "jclouds.pcloud.clientsecret";

    /**
     * Possible values for the property {@link Constants#PROPERTY_ENDPOINT}
     */
    public static final List<String> PROPERTY_PCLOUD_API_VALUES = Arrays.asList("api.pcloud.com", "eapi.pcloud.com");

    /**
     * Redis backend for metadata storage
     */
    public static final String PROPERTY_REDIS_CONNECT_STRING = "jclouds.redis";

    /**
     * Is support for user-defined metadata active?
     */
    public static final String PROPERTY_USERMETADATA_ACTIVE = "jclouds.pcloud.usermetadata.active";

    /**
     * Folder within the {@link #PROPERTY_BASEDIR} to store the user defined
     * metatdata
     */
    public static final String PROPERTY_USERMETADATA_FOLDER = "jclouds.pcloud.usermetadata.folder";

    /**
     * Interval in minutes the metadata folder is checked for orphaned entries
     * (metadata entries without corresponding file / folder)
     * if negative or 0, no scan occurs
     */
    public static final String PROPERTY_SANITIZE_METADATA_INTERVAL_MIN = "jclouds.pcloud.usermetadata.sanitize.interval";

    /**
     * Interval in minutes the content of the metadatafolder is synchronized with
     * the local cache.
     * if negative or 0, no sync occurs.
     */
    public static final String PROPERTY_SYNCHRONIZE_METADATA_INTERVAL_MIN = "jclouds.pcloud.usermetadata.synchronize.interval";

    private PCloudConstants() {
        throw new AssertionError("intentionally unimplemented");
    }
}
