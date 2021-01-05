package com.grax.aus;

import java.util.Calendar;
import java.util.TimeZone;

import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * https://developer.salesforce.com/docs/atlas.en-us.packagingGuide.meta/packagingGuide/app_analytics_managed_package_usage_summaries.htm
 * 
 * Package Usage Summaries are per month high level stats
 * 
 * @author troysellers
 *
 */
public class PackageUsageSummary {

	Dotenv dotenv = Dotenv.load();
	
	public static void main(String[] args) {
		try {
			PackageUsageSummary pls = new PackageUsageSummary();
			pls.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void run() throws Exception {
		
		PostgresUtils pgUtils = new PostgresUtils();
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("PST"));
		c.add(Calendar.MONTH, -1);
		
		
		if(!pgUtils.doRowsExist(c, AnalyticsRequestType.PACKAGE_USAGE_SUMMARY)) {
		
			PartnerConnection conn = SFUtil.getConnection(dotenv);
			SObject analyticsRequest = SFUtil.getAnalyticsRequest(AnalyticsRequestType.PACKAGE_USAGE_SUMMARY);
			analyticsRequest.setField("StartTime", c);
	
			
			SaveResult[] results = conn.create(new SObject[] {analyticsRequest});
			DataRetriever dr = new DataRetriever();
			
			for(SaveResult sr : results) {
				if (!sr.isSuccess()) {
					System.err.println("We have errors trying to create package usage summary request object");
					for (Error e : sr.getErrors()) {
						System.err.println(String.format("%s [%s]", e.getMessage(), e.getStatusCode().toString()));
					}
				} else { // we got our record created properly
					String recordId = sr.getId();
					String downloadUrl = dr.pollForDownloadUrl(recordId, conn);
					if (downloadUrl != null) {
							// file name is used to determine the table name as well. 
						String filePath = dr.downloadFile(downloadUrl, AnalyticsRequestType.PACKAGE_USAGE_SUMMARY.getFileName());
						pgUtils.copyPackageSummaryToPostgres(filePath);
					} else {
						System.err.println("We didn't get a download URL from the Salesforce API for Analytics Record ID "+recordId);
					}
				}
			}
		} else {
			System.out.println("We are skipping the fetch of the Package Usage Summary as we already have records for "+c.getTime()+" month");
		}
	}
}
