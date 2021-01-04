package com.grax.aus;

import java.io.IOException;
import java.util.Calendar;
import java.util.TimeZone;

import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;

import io.github.cdimascio.dotenv.Dotenv;

public class PackageUsageLog {

	Dotenv dotenv = Dotenv.load();
	
	public static void main(String[] args) {
		PackageUsageLog plf = new PackageUsageLog();
		try {
			plf.run();
		} catch (Throwable t) {
			t.printStackTrace();
			System.exit(1);
		}
	}
	
	public void run() throws ConnectionException, IOException {
		
		PartnerConnection conn = SFUtil.getConnection(dotenv);
		SObject analyticsRequest = SFUtil.getAnalyticsRequest(AnalyticsRequestType.PACKAGE_USAGE_LOG);
		PostgresUtils pgUtils = new PostgresUtils();
		
		Calendar startTime = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		startTime.add(Calendar.DAY_OF_MONTH, -2); 		
		analyticsRequest.setField("StartTime", startTime);
		
		SaveResult[] saveResults = conn.create(new SObject[] {analyticsRequest});
		DataRetriever dr = new DataRetriever();
		
		for(SaveResult sr : saveResults) {
			if(!sr.isSuccess()) {
				System.err.println("Unable to create analytics reqeust for "+AnalyticsRequestType.PACKAGE_USAGE_LOG);
				for(Error e : sr.getErrors()) {
					System.err.println(e.getMessage());
				}
			} else {
				String recordId = sr.getId();
				String downloadURL = dr.pollForDownloadUrl(recordId, conn);
				if(downloadURL != null) {
					String filePath = dr.downloadFile(downloadURL, "package_usage_log-"+System.currentTimeMillis()+".csv");
					pgUtils.copyPackageUsageLogToPostgres(filePath);
				}
			}
		}
		
	}
}