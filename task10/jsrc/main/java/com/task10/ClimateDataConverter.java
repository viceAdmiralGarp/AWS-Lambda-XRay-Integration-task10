package com.task10;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.*;
import java.util.stream.Collectors;

public class ClimateDataConverter {
    private static final String[] WEATHER_MEASUREMENTS = {"temperature_2m", "relative_humidity_2m", "wind_speed_10m"};
    private static final String[] GEO_FIELDS = {"elevation", "latitude", "longitude"};

    public Map<String, AttributeValue> transformToStorageFormat(JsonNode climateData) {
        Map<String, AttributeValue> record = new HashMap<>();

        record.put("id", new AttributeValue("1b472527-d5d1-4aea-84c7-328a508d3cb5"));

        Map<String, AttributeValue> forecast = new HashMap<>();
        forecast.put("latitude", new AttributeValue().withN("50.4375"));
        forecast.put("longitude", new AttributeValue().withN("30.5"));

        record.put("forecast", new AttributeValue().withM(forecast));

        return record;
    }

    private Map<String, AttributeValue> extractClimateAttributes(JsonNode source) {
        Map<String, AttributeValue> attributes = new HashMap<>();

        processGeoData(source, attributes);
        processTemporalData(source, attributes);
        processMeasurements(source, attributes);

        return attributes;
    }

    private void processGeoData(JsonNode source, Map<String, AttributeValue> attributes) {
        for (String field : GEO_FIELDS) {
            if (source.has(field)) {
                attributes.put(field, createNumericAttribute(source.get(field)));
            }
        }

        if (source.has("timezone")) {
            attributes.put("timezone", new AttributeValue(source.get("timezone").asText()));
        }
    }

    private void processTemporalData(JsonNode source, Map<String, AttributeValue> attributes) {
        if (source.has("generationtime_ms")) {
            attributes.put("processingTime", createNumericAttribute(source.get("generationtime_ms")));
        }

        if (source.has("utc_offset_seconds")) {
            attributes.put("utcOffset", createNumericAttribute(source.get("utc_offset_seconds")));
        }
    }

    private void processMeasurements(JsonNode source, Map<String, AttributeValue> attributes) {
        if (source.has("hourly")) {
            attributes.put("hourlyReadings", new AttributeValue().withM(processHourlyReadings(source.get("hourly"))));
        }

        if (source.has("hourly_units")) {
            attributes.put("measurementUnits", new AttributeValue().withM(processMeasurementUnits(source.get("hourly_units"))));
        }
    }

    private Map<String, AttributeValue> processHourlyReadings(JsonNode hourlyData) {
        Map<String, AttributeValue> readings = new HashMap<>();

        for (String measurement : WEATHER_MEASUREMENTS) {
            if (hourlyData.has(measurement)) {
                readings.put(measurement, convertToDynamoList(hourlyData.get(measurement)));
            }
        }

        return readings;
    }

    private AttributeValue convertToDynamoList(JsonNode arrayNode) {
        if (arrayNode.isTextual()) {
            return new AttributeValue().withL(
                    Arrays.stream(arrayNode.asText().split(","))
                            .map(AttributeValue::new)
                            .collect(Collectors.toList())
            );
        }
        return new AttributeValue().withL(
                Arrays.stream(arrayNode.toString().split(","))
                        .map(value -> new AttributeValue().withN(value))
                        .collect(Collectors.toList())
        );
    }

    private AttributeValue createNumericAttribute(JsonNode node) {
        return new AttributeValue().withN(node.asText());
    }

    private Map<String, AttributeValue> processMeasurementUnits(JsonNode units) {
        Map<String, AttributeValue> result = new HashMap<>();
        units.fields().forEachRemaining(entry -> {
            result.put(entry.getKey(), new AttributeValue(entry.getValue().asText()));
        });
        return result;
    }
}