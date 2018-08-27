package com.ywang.geocoding;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main method in this class gets addresses in the provided file, and send multiple geo-coordinate finding
 * requests concurrently to the Google Map API endpoint. The default max number of requests is 10.
 */
public class Geocoding {

    private static final Logger LOGGER = Logger.getLogger(Geocoding.class.getName());

    private static final String GOOGLE_MAP_URL = "https://maps.googleapis.com/maps/api/geocode/json";

    private static final int MAX_NUM_THREADS = 10;

    public static void main(String[] args) throws FileNotFoundException, InterruptedException, ExecutionException {
        System.out.println("Please input the path to the file containing addresses:");
        Scanner sc = new Scanner(System.in);
        List<String> addresses = readFile(sc.next());
        List<Future<JsonObject>> jsonResults = fanOutRequests(addresses);
        JsonArray output = new JsonArray();

        for (Future<JsonObject> future : jsonResults) {
            output.add(future.get());
        }
        System.out.println(output);
        System.exit(0);
    }

    /**
     * Fan out multiple location requests to the Google Map API and return a list of synchronous computations (Future
     * objects). The max number of concurrent requests is set by the const of MAX_NUM_THREADS.
     *
     * @param addresses a list of addresses passed to Google Map API;
     * @return a list of Future objects containing JsonObject results.
     */
    private static List<Future<JsonObject>> fanOutRequests(List<String> addresses) {
        ExecutorService threadPool = Executors.newFixedThreadPool(MAX_NUM_THREADS);
        List<Future<JsonObject>> futures = new ArrayList<>(addresses.size());

        for (String address : addresses) {
            Future<JsonObject> future = threadPool.submit(() -> {
                JsonObject entry = new JsonObject();
                entry.addProperty("address", address);
                Map<String, String> params = new HashMap<>();
                params.put("address", address);
                entry.addProperty("status", "FOUND");

                try {
                    HttpClient client = new HttpClient(GOOGLE_MAP_URL, params);
                    JsonObject result = client.invoke();

                    if ("OK".equals(result.get("status").getAsString())) {
                        JsonElement location =
                                result.getAsJsonArray("results").get(0).getAsJsonObject().getAsJsonObject("geometry")
                                        .get("location");
                        entry.add("location", location);
                    } else {
                        entry.addProperty("status", "NOT_FOUND");
                    }
                } catch (IOException e) {
                    entry.addProperty("status", "NOT_FOUND");
                    LOGGER.log(Level.SEVERE, "Exception occurs: ", e);
                }
                return entry;
            });
            futures.add(future);
        }
        return futures;
    }

    /**
     * Get the contents of the provided file.
     *
     * @param filePath the path to the file;
     * @return a list of string, each one corresponds to a line in the file.
     */
    private static List<String> readFile(String filePath) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(filePath));
        List<String> res = new LinkedList<>();
        while (sc.hasNextLine()) {
            String next = sc.nextLine();
            if (next.length() != 0) {
                res.add(next);
            }
        }
        return res;
    }
}
