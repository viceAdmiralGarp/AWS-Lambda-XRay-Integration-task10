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
    private static final double DEFAULT_LATITUDE = 50.4375;
    private static final double DEFAULT_LONGITUDE = 30.5;

    private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
    private final MeteorologyService weatherAPI = new MeteorologyService();
    private final ClimateDataConverter dataMapper = new ClimateDataConverter();

    @Override
    public Map<String, Object> handleRequest(Object request, Context context) {
        try {
            JsonNode weatherData = weatherAPI.fetchWeather(DEFAULT_LATITUDE, DEFAULT_LONGITUDE);
            Map<String, AttributeValue> dynamoItem = dataMapper.convertToDynamoDBItem(weatherData);

            String dynamoTableName = System.getenv("table");
            dynamoDBClient.putItem(dynamoTableName, dynamoItem);

            return generateSuccessResponse();
        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage());
            return generateErrorResponse(e.getMessage());
        }
    }

    private Map<String, Object> generateErrorResponse(String errorMessage) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("statusCode", 500);
        errorResponse.put("body", "Error processing weather data: " + errorMessage);
        return errorResponse;
    }

    private Map<String, Object> generateSuccessResponse() {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", 200);
        response.put("body", "Weather forecast successfully saved to DynamoDB");
        return response;
    }
}