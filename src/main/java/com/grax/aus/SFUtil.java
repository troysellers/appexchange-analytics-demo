package com.grax.aus;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import io.github.cdimascio.dotenv.Dotenv;

public class SFUtil {

	
	public static PartnerConnection getConnection(Dotenv dotenv) throws ConnectionException {
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(dotenv.get("SF_USER"));
		config.setPassword(dotenv.get("SF_PASS"));
		config.setPrettyPrintXml(true);
		config.setTraceMessage(Boolean.valueOf(dotenv.get("SF_SOAP_TRACE")));
		config.setCompression(true);
		config.setAuthEndpoint(dotenv.get("SF_ENDPOINT"));
		
		return Connector.newConnection(config);
	}
	
	/**
	 * Returns an SObject with data type
	 * https://developer.salesforce.com/docs/atlas.en-us.228.0.object_reference.meta/object_reference/sforce_api_objects_appanalyticsqueryrequest.htm
	 * @param requestType
	 * @return
	 */
	public static SObject getAnalyticsRequest(AnalyticsRequestType requestType) {
		
		SObject appAnalyticsQueryRequest = new SObject();
		appAnalyticsQueryRequest.setType("AppAnalyticsQueryRequest");
		appAnalyticsQueryRequest.setField("DataType", requestType.getValue());
		
		return appAnalyticsQueryRequest;
	}
}
