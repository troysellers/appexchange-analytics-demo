package com.grax.aus;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;

import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import io.github.cdimascio.dotenv.Dotenv;

public class SubscriberSnapshot {

	Dotenv dotenv = Dotenv.load();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SubscriberSnapshot ss = new SubscriberSnapshot();
		try {
			ss.run();
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}
	}

	public void run() throws ConnectionException, IOException, SQLException {
		
		PostgresUtils pgUtils = new PostgresUtils();
		Calendar endTime = Calendar.getInstance(TimeZone.getTimeZone("PST"));
		Calendar startTime = Calendar.getInstance(TimeZone.getTimeZone("PST"));
		
		endTime.add(Calendar.DAY_OF_MONTH, -1);
		startTime.add(Calendar.DAY_OF_MONTH, -2);
		
		
		if(!pgUtils.doRowsExist(endTime, AnalyticsRequestType.SUBSCRIBER_SNAPSHOT)) {
			PartnerConnection conn = SFUtil.getConnection(dotenv);
			SObject analyticsRequest = SFUtil.getAnalyticsRequest(AnalyticsRequestType.SUBSCRIBER_SNAPSHOT);
			
			
			analyticsRequest.setField("EndTime", endTime);
			analyticsRequest.setField("StartTime", startTime);
			
			SaveResult[] saveResults = conn.create(new SObject[] {analyticsRequest});
			DataRetriever dr = new DataRetriever();
			
			for(SaveResult sr : saveResults) {
				if(!sr.isSuccess()) {
					System.err.println("We were unable to create an AnalyticsRequest object");
					for(Error e : sr.getErrors()) {
						System.err.println(e.getMessage());
					}
				} else {
					String recordId = sr.getId();
					String downloadUrl = dr.pollForDownloadUrl(recordId, conn);
					if(downloadUrl != null) {
						String filePath = dr.downloadFile(downloadUrl, AnalyticsRequestType.SUBSCRIBER_SNAPSHOT.getFileName());
						pgUtils.copySubscriberSnapshotToPostgres(filePath);
					} else {
						System.err.println("We didn't get a download URL from the Salesforce API for Analytics Record ID "+recordId);
					}
				}
			}
		} else {
			System.out.println("We are skipping collection of Subscriber Summary logs because we have records for the day "+endTime.getTime());
		}
		
	}
}
