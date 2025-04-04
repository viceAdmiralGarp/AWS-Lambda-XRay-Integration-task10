package com.task10;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import java.util.stream.Collectors;

public class ClimateDataConverter {
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public Map<String, AttributeValue> convertToDynamoDBItem(JsonNode weatherResponse) {
        Map<String, AttributeValue> dynamoItem = new HashMap<>();
        dynamoItem.put("id", new AttributeValue(UUID.randomUUID().toString()));
        dynamoItem.put("forecast", new AttributeValue().withM(createForecast(weatherResponse)));
        return dynamoItem;
    }

    private Map<String, AttributeValue> createForecast(JsonNode weatherResponse) {
        Map<String, AttributeValue> forecast = new HashMap<>();
        if (weatherResponse == null) {
            return forecast;
        }

        forecast.put("elevation", new AttributeValue().withN(String.valueOf(weatherResponse.get("elevation") != null ? weatherResponse.get("elevation").asDouble() : 0.0)));
        forecast.put("generation_time_ms", new AttributeValue().withN(String.valueOf(weatherResponse.get("generation_time_ms") != null ? weatherResponse.get("generation_time_ms").asDouble() : 0.0)));
        forecast.put("latitude", new AttributeValue().withN(String.valueOf(weatherResponse.get("latitude") != null ? weatherResponse.get("latitude").asDouble() : 0.0)));
        forecast.put("longitude", new AttributeValue().withN(String.valueOf(weatherResponse.get("longitude") != null ? weatherResponse.get("longitude").asDouble() : 0.0)));
        forecast.put("timezone", new AttributeValue(weatherResponse.get("timezone") != null ? weatherResponse.get("timezone").asText() : ""));
        forecast.put("timezone_abbr", new AttributeValue(weatherResponse.get("timezone_abbr") != null ? weatherResponse.get("timezone_abbr").asText() : ""));
        forecast.put("utc_offset_sec", new AttributeValue().withN(String.valueOf(weatherResponse.get("utc_offset_sec") != null ? weatherResponse.get("utc_offset_sec").asInt() : 0)));

        JsonNode hourlyData = weatherResponse.get("hourly");
        if (hourlyData != null) {
            forecast.put("hourly", new AttributeValue().withM(createHourlyData(hourlyData)));
        }

        JsonNode hourlyUnits = weatherResponse.get("hourly_units");
        if (hourlyUnits != null) {
            forecast.put("hourly_units", new AttributeValue().withM(createHourlyUnits(hourlyUnits)));
        }

        return forecast;
    }

    private Map<String, AttributeValue> createHourlyData(JsonNode hourlyData) {
        Map<String, AttributeValue> hourly = new HashMap<>();
        hourly.put("temperature_2m", new AttributeValue().withL(
                Arrays.stream(jsonMapper.convertValue(hourlyData.get("temperature_2m"), double[].class))
                        .mapToObj(d -> new AttributeValue().withN(String.valueOf(d)))
                        .collect(Collectors.toList())
        ));
        hourly.put("time", new AttributeValue().withL(
                Arrays.stream(jsonMapper.convertValue(hourlyData.get("time"), String[].class))
                        .map(AttributeValue::new)
                        .collect(Collectors.toList())
        ));
        return hourly;
    }

    private Map<String, AttributeValue> createHourlyUnits(JsonNode unitsNode) {
        Map<String, AttributeValue> hourlyUnits = new HashMap<>();
        hourlyUnits.put("temperature_2m", new AttributeValue(unitsNode.get("temperature_2m") != null ? unitsNode.get("temperature_2m").asText() : ""));
        hourlyUnits.put("time", new AttributeValue(unitsNode.get("time") != null ? unitsNode.get("time").asText() : ""));
        return hourlyUnits;
    }
}