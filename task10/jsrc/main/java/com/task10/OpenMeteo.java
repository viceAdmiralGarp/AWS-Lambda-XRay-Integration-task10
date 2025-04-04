package com.task10;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class OpenMeteo {
    private final HttpClient apiClient = HttpClient.newHttpClient();
    private final ObjectMapper jsonParser = new ObjectMapper();
    private static final String API_ENDPOINT = "https://api.open-meteo.com/v1/forecast?latitude=%.2f&longitude=%.2f&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m";

    public JsonNode getWeather(double lat, double lng) throws Exception {
        String apiUrl = String.format(API_ENDPOINT, lat, lng);

        HttpRequest apiRequest = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .GET()
                .build();

        HttpResponse<String> apiResponse = apiClient.send(apiRequest, HttpResponse.BodyHandlers.ofString());
        return jsonParser.readTree(apiResponse.body());
    }
}