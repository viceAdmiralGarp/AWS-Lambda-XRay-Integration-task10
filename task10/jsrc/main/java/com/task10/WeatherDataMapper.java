package com.task10;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class WeatherDataMapper {
    private final ObjectMapper jsonParser = new ObjectMapper();

    public Map<String, AttributeValue> convertToDynamoItem(JsonNode weatherInfo) {
        Map<String, AttributeValue> dbItem = new HashMap<>();
        dbItem.put("id", new AttributeValue(UUID.randomUUID().toString()));
        dbItem.put("forecast", new AttributeValue().withM(createWeatherForecast(weatherInfo)));
        return dbItem;
    }

    private Map<String, AttributeValue> createWeatherForecast(JsonNode weatherInfo) {
        Map<String, AttributeValue> weatherForecast = new HashMap<>();
        if (weatherInfo == null) {
            return weatherForecast;
        }

        weatherForecast.put("elevation", new AttributeValue().withN(String.valueOf(weatherInfo.get("elevation") != null ? weatherInfo.get("elevation").asDouble() : 0.0)));
        weatherForecast.put("generationtime_ms", new AttributeValue().withN(String.valueOf(weatherInfo.get("generationtime_ms") != null ? weatherInfo.get("generationtime_ms").asDouble() : 0.0)));
        weatherForecast.put("latitude", new AttributeValue().withN(String.valueOf(weatherInfo.get("latitude") != null ? weatherInfo.get("latitude").asDouble() : 0.0)));
        weatherForecast.put("longitude", new AttributeValue().withN(String.valueOf(weatherInfo.get("longitude") != null ? weatherInfo.get("longitude").asDouble() : 0.0)));
        weatherForecast.put("timezone", new AttributeValue(weatherInfo.get("timezone") != null ? weatherInfo.get("timezone").asText() : ""));
        weatherForecast.put("timezone_abbreviation", new AttributeValue(weatherInfo.get("timezone_abbreviation") != null ? weatherInfo.get("timezone_abbreviation").asText() : ""));
        weatherForecast.put("utc_offset_seconds", new AttributeValue().withN(String.valueOf(weatherInfo.get("utc_offset_seconds") != null ? weatherInfo.get("utc_offset_seconds").asInt() : 0)));

        JsonNode hourlyInfo = weatherInfo.get("hourly");
        if (hourlyInfo != null) {
            weatherForecast.put("hourly", new AttributeValue().withM(createHourlyData(hourlyInfo)));
        }

        JsonNode hourlyUnitsInfo = weatherInfo.get("hourly_units");
        if (hourlyUnitsInfo != null) {
            weatherForecast.put("hourly_units", new AttributeValue().withM(createHourlyUnits(hourlyUnitsInfo)));
        }

        return weatherForecast;
    }

    private Map<String, AttributeValue> createHourlyData(JsonNode hourlyInfo) {
        Map<String, AttributeValue> hourlyValues = new HashMap<>();
        hourlyValues.put("temperature_2m", new AttributeValue().withL(
                Arrays.stream(jsonParser.convertValue(hourlyInfo.get("temperature_2m"), double[].class))
                        .mapToObj(val -> new AttributeValue().withN(String.valueOf(val)))
                        .collect(Collectors.toList())
        ));
        hourlyValues.put("time", new AttributeValue().withL(
                Arrays.stream(jsonParser.convertValue(hourlyInfo.get("time"), String[].class))
                        .map(AttributeValue::new)
                        .collect(Collectors.toList())
        ));
        return hourlyValues;
    }

    private Map<String, AttributeValue> createHourlyUnits(JsonNode unitsInfo) {
        Map<String, AttributeValue> unitValues = new HashMap<>();
        unitValues.put("temperature_2m", new AttributeValue(unitsInfo.get("temperature_2m") != null ? unitsInfo.get("temperature_2m").asText() : ""));
        unitValues.put("time", new AttributeValue(unitsInfo.get("time") != null ? unitsInfo.get("time").asText() : ""));
        return unitValues;
    }
}