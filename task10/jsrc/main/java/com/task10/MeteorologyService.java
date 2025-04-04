package com.task10;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

public class MeteorologyService {
    private static final String API_BASE_URL = "https://api.open-meteo.com/v1/forecast";
    private final HttpClient client;
    private final ObjectMapper jsonParser;

    public MeteorologyService() {
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.jsonParser = new ObjectMapper();
    }

    public JsonNode retrieveAtmosphericConditions(double lat, double lng) throws AtmosphericDataException {
        try {
            String apiEndpoint = buildRequestUrl(lat, lng);
            HttpRequest apiRequest = createHttpRequest(apiEndpoint);
            HttpResponse<String> apiResponse = executeRequest(apiRequest);
            return parseResponse(apiResponse);
        } catch (Exception e) {
            throw new AtmosphericDataException("Failed to retrieve weather data", e);
        }
    }

    private String buildRequestUrl(double lat, double lng) {
        return String.format("%s?latitude=%.4f&longitude=%.4f&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m",
                API_BASE_URL, lat, lng);
    }

    private HttpRequest createHttpRequest(String url) {
        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/json")
                .GET()
                .build();
    }

    private HttpResponse<String> executeRequest(HttpRequest request) throws Exception {
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private JsonNode parseResponse(HttpResponse<String> response) throws Exception {
        return jsonParser.readTree(response.body());
    }

    public static class AtmosphericDataException extends Exception {
        public AtmosphericDataException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}