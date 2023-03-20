package com.github.stefanrichterhuber.pCloudForjClouds.blobstore.internal;

import java.util.List;

import com.google.gson.annotations.Expose;

/**
 * 
 * Response for the getapi call
 * {
 * "result": 0,
 * "binapi": [
 * "binapi-ams1.pcloud.com"
 * ],
 * "api": [
 * "api-ams1.pcloud.com"
 * ]
 * }
 * 
 * @author stefan
 *
 */
public class GetApiResponse {
    @Expose
    private int result;

    @Expose
    private List<String> binapi;

    @Expose
    private List<String> api;

    /**
     * Any other result than 0 indicates an error
     * 
     * @return
     */
    public int getResult() {
        return result;
    }

    public boolean isOk() {
        return getResult() == 0;
    }

    public List<String> getBinapi() {
        return binapi;
    }

    public List<String> getApi() {
        return api;
    }

    @Override
    public String toString() {
        return "GetApiResponse [result=" + result + ", binapi=" + binapi + ", api=" + api + "]";
    }
}
