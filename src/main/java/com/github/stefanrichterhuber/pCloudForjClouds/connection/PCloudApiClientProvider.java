package com.github.stefanrichterhuber.pCloudForjClouds.connection;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.GetApiResponse;
import com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal.PCloudUtils;
import com.github.stefanrichterhuber.pCloudForjClouds.reference.PCloudConstants;
import com.google.inject.Provider;
import com.pcloud.sdk.ApiClient;
import com.pcloud.sdk.Authenticators;
import com.pcloud.sdk.PCloudSdk;

/**
 * Provider for PCloud {@link ApiClient}s.
 * 
 * @author Stefan Richter-Huber
 *
 */
public class PCloudApiClientProvider implements Provider<ApiClient> {
    private final static Logger LOGGER = LoggerFactory.getLogger(PCloudApiClientProvider.class);

    @Nonnull
    private final String clientSecret;
    @Nonnull
    private final String pCloudHost;

    /**
     * Cache the ApiClient for each client secret, because one should pool the
     * {@link ApiClient}s.
     */
    private final static Map<String, ApiClient> API_CLIENTS = new ConcurrentHashMap<>();

    @Inject
    protected PCloudApiClientProvider(
            @Named(PCloudConstants.PROPERTY_CLIENT_SECRET) String clientSecret, //
            // @Named(Constants.PROPERTY_ENDPOINT) String pCloudHost //
            Properties props) {
        this.clientSecret = checkNotNull(clientSecret, "Property " + PCloudConstants.PROPERTY_CLIENT_SECRET);
        // pCloudHost = pCloudHost != null ? pCloudHost :
        // PCloudUtils.testForAPIEndpoint(clientSecret).orNull();
        // this.pCloudHost = checkNotNull(pCloudHost, "Property " +
        // Constants.PROPERTY_ENDPOINT);
        // this.pCloudHost = PCloudUtils.testForAPIEndpoint(clientSecret).orNull();
        this.pCloudHost = PCloudUtils.testForAPIEndpoint(clientSecret).get();
    }

    /**
     * Factory for {@link ApiClient}s for a given client secret.
     * 
     * @param pCloudHost   API Host
     * @param clientSecret client secret
     * @return {@link ApiClient}
     */
    public static final ApiClient create(@Nonnull String pCloudHost, @Nonnull String clientSecret) {
        final GetApiResponse apiResponse = PCloudUtils.getApiServer(pCloudHost).join();
        if (apiResponse != null && apiResponse.getResult() == 0 && apiResponse.getApi().size() > 0
                && !pCloudHost.equals(apiResponse.getApi().get(0))) {
            final String closestApi = apiResponse.getApi().get(0);
            LOGGER.info("Determined nearest API endpoint '{}' for general API endpoint '{}'",
                    closestApi,
                    pCloudHost);
            pCloudHost = closestApi != null ? closestApi : pCloudHost;
        }

        return PCloudSdk.newClientBuilder().apiHost(pCloudHost)
                .authenticator(Authenticators.newOAuthAuthenticator(clientSecret)).create();
    }

    @Override
    public ApiClient get() {
        final ApiClient result = API_CLIENTS.computeIfAbsent(this.clientSecret,
                clientSecret -> create(pCloudHost, clientSecret));
        return result;
    }

}
