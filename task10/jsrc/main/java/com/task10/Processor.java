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

	private static final double AREA_LATITUDE = 50.4375;
	private static final double AREA_LONGITUDE = 30.5;

	private final AmazonDynamoDB storageClient;
	private final MeteorologyService atmosphericService;
	private final ClimateDataConverter dataAdapter;

	public Processor() {
		this.storageClient = AmazonDynamoDBClientBuilder.standard().build();
		this.atmosphericService = new MeteorologyService();
		this.dataAdapter = new ClimateDataConverter();
	}

	private Map<String, Object> createResponse(int status, String message) {
		Map<String, Object> result = new HashMap<>();
		result.put("statusCode", status);
		result.put("body", message);
		return result;
	}

	@Override
	public Map<String, Object> handleRequest(Object input, Context context) {
		try {
			JsonNode atmosphericData = atmosphericService.retrieveAtmosphericConditions(
					AREA_LATITUDE,
					AREA_LONGITUDE
			);

			Map<String, AttributeValue> storageItem = dataAdapter.transformToStorageFormat(atmosphericData);

			String targetTable = System.getenv("table");
			storageClient.putItem(targetTable, storageItem);

			return createResponse(200, "Atmospheric data successfully recorded");

		} catch (MeteorologyService.AtmosphericDataException e) {
			context.getLogger().log("Data acquisition error: " + e.getMessage());
			return createResponse(502, "Failed to acquire atmospheric data");
		} catch (Exception e) {
			context.getLogger().log("Processing failure: " + e.getMessage());
			return createResponse(500, "Data processing error occurred");
		}
	}
}