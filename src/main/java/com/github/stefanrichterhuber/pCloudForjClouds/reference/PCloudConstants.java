package com.github.stefanrichterhuber.pCloudForjClouds.reference;

import java.util.Arrays;
import java.util.List;

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
     * Possible values for the property {@link #PROPERTY_PCLOUD_API}
     */
    public static final List<String> PROPERTY_PCLOUD_API_VALUES = Arrays.asList("api.pcloud.com", "eapi.pcloud.com");

    /**
     * API backend for pCloud. Either api.pcloud.com or eapi.pcloud.com
     */
    public static final String PROPERTY_PCLOUD_API = "jclouds.pcloud.api";

    /**
     * Is support for user-defined metadata active?
     */
    public static final String PROPERTY_USERMETADATA_ACTIVE = "jclouds.pcloud.usermetadata.active";

    /**
     * Folder within the {@link #PROPERTY_BASEDIR} to store the user defined
     * metatdata
     */
    public static final String PROPERTY_USERMETADATA_FOLDER = "jclouds.pcloud.usermetadata.folder";

    private PCloudConstants() {
        throw new AssertionError("intentionally unimplemented");
    }
}
