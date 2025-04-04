package com.task10;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.lambda.LambdaUrlConfig;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.TracingMode;
import com.syndicate.deployment.model.lambda.url.AuthType;
import com.syndicate.deployment.model.lambda.url.InvokeMode;

import java.util.HashMap;
import java.util.Map;

@LambdaHandler(
        lambdaName = "processor",
        roleName = "processor-role",
        isPublishVersion = true,
        aliasName = "${lambdas_alias_name}",
        logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED,
        tracingMode = TracingMode.Active
)
@LambdaUrlConfig(
        authType = AuthType.NONE,
        invokeMode = InvokeMode.BUFFERED
)
@EnvironmentVariables(value = {@EnvironmentVariable(key = "table", value = "${target_table}")})
public class Processor implements RequestHandler<Object, Map<String, Object>> {

    private static final double DEFAULT_LONG = 30.5;
    private static final double DEFAULT_LAT = 50.4375;

    private final Client weatherService = new Client();
    private final Mapper weatherMapper = new Mapper();
    private final AmazonDynamoDB dynamoClient = AmazonDynamoDBClientBuilder.standard().build();

    @Override
    public Map<String, Object> handleRequest(Object input, Context context) {
        try {
            JsonNode weatherInfo = weatherService.getWeather(DEFAULT_LAT, DEFAULT_LONG);
            Map<String, AttributeValue> dbItem = weatherMapper.convertToDynamoItem(weatherInfo);

            String dynamoTable = System.getenv("table");
            dynamoClient.putItem(dynamoTable, dbItem);

            return createSuccessResult();
        } catch (Exception ex) {
            context.getLogger().log("Error occurred: " + ex.getMessage());
            return createErrorResult(ex.getMessage());
        }
    }

    private Map<String, Object> createSuccessResult() {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", 200);
        response.put("body", "Weather data stored successfully");
        return response;
    }

    private Map<String, Object> createErrorResult(String errorMsg) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("statusCode", 500);
        errorResponse.put("body", "Failed to process weather data: " + errorMsg);
        return errorResponse;
    }
}