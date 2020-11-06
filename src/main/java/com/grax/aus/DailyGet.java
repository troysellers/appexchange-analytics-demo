package com.grax.aus;

import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import io.github.cdimascio.dotenv.Dotenv;

public class DailyGet {
	private Dotenv dotenv = Dotenv.load();
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DailyGet dg = new DailyGet();
		try {
			dg.run();
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void run() {
		AppAnalytics aa = new AppAnalytics();
		
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(dotenv.get("SF_USER"));
		config.setPassword(dotenv.get("SF_PASS"));
		config.setPrettyPrintXml(true);
		config.setTraceMessage(Boolean.valueOf(dotenv.get("SF_SOAP_TRACE")));
		config.setCompression(true);
		config.setAuthEndpoint(dotenv.get("SF_ENDPOINT"));
		
		
		try {
			PartnerConnection pc = Connector.newConnection(config);		
			System.out.println("Created connection to Salesforce for "+pc.getUserInfo().getUserFullName());
			SObject appAnalyticsQueryRequest = new SObject();
			appAnalyticsQueryRequest.setType("AppAnalyticsQueryRequest");
			appAnalyticsQueryRequest.setField("DataType", "PackageUsageLog");
			Calendar c = Calendar.getInstance(TimeZone.getTimeZone("PST"));
			String filePath = "";
			
			//case "PackageUsageLog":
			c.add(Calendar.DATE, -1); // yesterday should be the latest
			appAnalyticsQueryRequest.setField("StartTime", c);
			filePath = aa.packageUsageTable+"-"+c.getTimeInMillis()+".csv";
			aa.fetchAndWrite(c, appAnalyticsQueryRequest, filePath, pc);
						
			//case "SubscriberSnapshot":
			appAnalyticsQueryRequest.setField("DataType", "SubscriberSnapshot");
			appAnalyticsQueryRequest.setField("EndTime", c);
			appAnalyticsQueryRequest.setField("StartTime", c);
			filePath = aa.subscriberSnapshotTable+"-"+c.getTimeInMillis()+".csv";
			aa.fetchAndWrite(c, appAnalyticsQueryRequest, filePath, pc);
	
			PostgresCopy pgc = new PostgresCopy();
			pgc.run();

		} catch (ConnectionException ce) {
			ce.printStackTrace();
			System.exit(1);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		 
	}
}
