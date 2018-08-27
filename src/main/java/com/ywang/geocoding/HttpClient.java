package com.ywang.geocoding;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is used to send a basic HTTP Get request. The following rules are applied to each invoke:
 * <p>
 * - When invoke method is called, a Get request will be sent. If it's failed, the program will wait for a bit and
 * send the same request again. The default number of retries is 5.
 * <p>
 * - The default timeout is 30 seconds.
 */
public class HttpClient {
    private static final Logger LOGGER = Logger.getLogger(HttpClient.class.getName());

    private static final String CHAR_SET = StandardCharsets.UTF_8.name();
    private static final String OK_STATUS = "OK";
    private static final String STATUS_KEY = "status";

    private static final int MAX_NUM_RETRIES = 5;
    private static final int BACKOFF_FACTOR = 5;
    private static final int TIMEOUT = 30_000;

    private static final Random RANDOM_GENERATOR = new Random();

    private final URL url;

    public HttpClient(String host, Map<String, String> params) throws IOException {
        StringBuilder queries = new StringBuilder();
        for (String key : params.keySet()) {
            queries.append(key);
            queries.append("=");
            queries.append(URLEncoder.encode(params.get(key), CHAR_SET));
            queries.append("&");
        }
        url = new URL(host + "?" + queries.substring(0, queries.length() - 1));
    }

    public HttpClient(String host) throws MalformedURLException {
        url = new URL(host);
    }

    /**
     * Invoke a HTTP Get request. If failed, it will retry until the max number of retries is reached.
     *
     * @return a JsonObject returned from the endpoint.
     * @throws IOException when a connection cannot be made.
     */
    public JsonObject invoke() throws IOException {
        int numRetries = 1;
        JsonObject response = getRequest();

        while (!OK_STATUS.equals(response.get(STATUS_KEY).getAsString()) && numRetries < MAX_NUM_RETRIES) {
            // Wait for a random number of seconds before a retry. The wait time becomes longer (not guaranteed) with
            // the number of retries.
            int sleepSecs = RANDOM_GENERATOR.nextInt(numRetries * BACKOFF_FACTOR) + 1;

            try {
                TimeUnit.SECONDS.sleep(sleepSecs);
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "The thread sleep is interrupted.");
            }
            response = getRequest();
            numRetries++;
        }
        return response;
    }

    private JsonObject getRequest() throws IOException {
        URLConnection connection = url.openConnection();
        connection.setRequestProperty("Accept-Charset", CHAR_SET);
        connection.setConnectTimeout(TIMEOUT);
        final InputStreamReader inputStreamReader = new InputStreamReader(connection.getInputStream());
        return new JsonParser().parse(inputStreamReader).getAsJsonObject();
    }

}
