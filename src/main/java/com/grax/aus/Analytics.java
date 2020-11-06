package com.grax.aus;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import io.github.cdimascio.dotenv.Dotenv;

public class Analytics {

	private PartnerConnection conn;
	private Dotenv dotenv = Dotenv.load();
	private String tmpDir = "/tmp/"; // used for download analytics files to, files should be cleaned after load.
	
	protected String packageUsageTable = "package_usage_log";
	protected String packageUsageSummaryTable = "package_usage_summary";
	protected String subscriberSnapshotTable = "subscriber_snapshot";
	
	public static void main(String[] args) {

		try {
			Analytics a = new Analytics();
			a.run(args);
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private void run(String[] args) throws Exception {
			
		createSFConnection();
		
		
		// do we need this months package summary? 
		if(!lastMonthSummaryExists()) {
			String packageSummaryURL = fetchPackageUsageSummaries();
			String filename = "package_usage_summary-"+System.currentTimeMillis()+".csv";
			
			// download the file
			downloadFile(packageSummaryURL, tmpDir + filename);
		   
		   // copy into postgres
			long bytesCopied = copyToPostgres(tmpDir + filename, "package_usage_summary");
			if(bytesCopied > 0 ) {
				Files.delete(Paths.get(tmpDir+filename));
			}
		} else {
			System.out.println("Skipping fetch of package summary for this month as it seems like we already have it");
		}
		
		// get all the distinct org id's for this months package summary
		String[] orgIds = getOrgIds();
	
		// if we don't have any package usage log tables for yesterday, go get em
		if(!latestPackageUsageExists()) {
			String[] packageUsageLogDownloads = fetchPackageUsageLogForOrgID(orgIds);
			for(String dl : packageUsageLogDownloads) {
				String packageUsageFile = tmpDir + "package_usage_log"+System.currentTimeMillis()+".csv";
				downloadFile(dl, packageUsageFile);
				long rowsCopied = copyToPostgres(packageUsageFile, "package_usage_log");
				if (rowsCopied > 0) {
					Files.delete(Paths.get(packageUsageFile));
				}
			}
		} else {
			System.out.println("We have package usage logs for today... skipping");
		}
		
		String[] subscriberSummaryDownloads = fetchSubscriberSummaries(orgIds);
		for(String dl : subscriberSummaryDownloads) {
			String subscriberSummaryFile = tmpDir + "subscriber_summary"+System.currentTimeMillis()+".csv";
			downloadFile(dl, subscriberSummaryFile);
			long subscriberRecordsCopied = copyToPostgres(subscriberSummaryFile, "subscriber_snapshot");
			if (subscriberRecordsCopied > 0) {
				Files.delete(Paths.get(subscriberSummaryFile));
			}
		}
	}

	/*
	 * package summaries are generated once per month in Salesforce, so the database has a simple column that is just year - month (yyyy-MM)
	 * This method looks for the latest package summary month and tests if it is equal to this month. 
	 * 
	 * It assumes that the latest month in the database will only ever be older or current, never in the future.
	 */
	private boolean lastMonthSummaryExists() {
		
		boolean retval = false;
		
		try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))){
			PreparedStatement ps = conn.prepareStatement("select distinct(month) from package_usage_summary order by 1 desc limit 1" );
			ResultSet rs = ps.executeQuery();
			if(rs.next()) {
				String monthVal = rs.getString(1);
				System.out.println("monthval "+monthVal);
				String year = monthVal.substring(0, monthVal.indexOf("-"));
				String month = monthVal.substring(monthVal.indexOf("-")+1, monthVal.length());
				System.out.println(String.format("Year %s month %s", year, month));
				Calendar c = Calendar.getInstance(TimeZone.getTimeZone("PST"));
				
			
				if(Integer.valueOf(year) == c.get(Calendar.YEAR) && Integer.valueOf(month) == c.get(Calendar.MONTH)) { //this is last month as salesforce has Jan = 1 and java calendar has jan = 0
					retval = true;
				}
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
		System.out.println("Last months summary exists ["+retval+"]");
		return retval;
	}
	/*
	 * returns the download URLS for the package summaries
	 */
	private String fetchPackageUsageSummaries() throws Exception{
		
		SObject packageSummariesRequest = getAppAnalyticsRequest("PackageUsageSummary");
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("PST")); // it's Salesforce so pretend we are running this in San Francisco..
		c.add(Calendar.MONTH, -1); // package summaries are only generate for a full month, so let's get last months
		packageSummariesRequest.setField("StartTime", c); 

		SaveResult[] results = conn.create(new SObject[] {packageSummariesRequest});
		
		System.out.println("created a new job for "+packageSummariesRequest.getType());
		
		for(SaveResult sr : results) {
			String recordId = sr.getId();
			
			if(recordId != null) {
				String downloadUrl = pollForDownloadUrl(recordId);
				if (downloadUrl != null && !downloadUrl.isEmpty()) {
					return downloadUrl;
				}
			} else {
					System.out.println("We have a null record id? ");
			}
		}	
		
		return "";
	}
	
	/*
	 * returns the download URL for package usage data
	 */
	private String[] fetchPackageUsageLogForOrgID(String[] orgIds) throws Exception {
		
		List<SObject> sobjectArray = new ArrayList<>();
		List<String> downloadUrls = new ArrayList<>();
		for(String orgId : orgIds) {
			SObject packageLogRequest = getAppAnalyticsRequest("PackageUsageLog");
			Calendar c = Calendar.getInstance(TimeZone.getTimeZone("PST"));
			c.add(Calendar.DAY_OF_YEAR, -1); 
			StringBuilder b = new StringBuilder(orgId);
			for(int i=0 ; i<15; i++) {
				b.append(",");
				b.append(orgId);
			}
			packageLogRequest.setField("StartTime", c);
			packageLogRequest.setField("OrganizationIds", b.toString());
			sobjectArray.add(packageLogRequest);
		}
		SaveResult[] results = conn.create(sobjectArray.toArray(new SObject[0]));
		
		for(SaveResult sr : results) {
			String recordId = sr.getId();
			String downloadUrl = pollForDownloadUrl(recordId);
			if(downloadUrl != null && !downloadUrl.isEmpty()) {
				downloadUrls.add(downloadUrl);
			}
		}
		
		return downloadUrls.toArray(new String[0]);
		
	}
	
	private String[] fetchSubscriberSummaries(String[] orgIds) throws Exception {
		
		List<SObject> sobjectArray = new ArrayList<>();
		List<String> downloadURLs = new ArrayList<>();
		
		for(String orgId : orgIds) {
			
			SObject subscriberSnapshot = getAppAnalyticsRequest("SubscriberSnapshot");
			Calendar c = Calendar.getInstance(TimeZone.getTimeZone("PST"));
			c.add(Calendar.DAY_OF_YEAR, -1);
			StringBuilder b = new StringBuilder(orgId);
			for(int i=0 ; i< 15 ; i++) {
				b.append(",");
				b.append(orgId);
			}
			subscriberSnapshot.setField("StartTime", c);
			subscriberSnapshot.setField("OrganizationIds", b.toString());
			sobjectArray.add(subscriberSnapshot);
			
		}
		SaveResult[] results = conn.create(sobjectArray.toArray(new SObject[0]));
		
		for(SaveResult sr : results) {
			String recordId = sr.getId();
			String downloadUrl = pollForDownloadUrl(recordId);
			if (downloadUrl != null && !downloadUrl.isEmpty()) {
				downloadURLs.add(downloadUrl);
			}
		}
		 
		return downloadURLs.toArray(new String[0]);
	}
	/*
	 * looks for system environment to create a connection to the Salesforce instance. 
	 */
	private void createSFConnection() throws Exception{
		if (conn == null) {
			ConnectorConfig config = new ConnectorConfig();
			config.setUsername(dotenv.get("SF_USER"));
			config.setPassword(dotenv.get("SF_PASS"));
			config.setPrettyPrintXml(true);
			config.setTraceMessage(Boolean.valueOf(dotenv.get("SF_SOAP_TRACE")));
			config.setCompression(true);
			config.setAuthEndpoint(dotenv.get("SF_ENDPOINT"));
			
			conn = Connector.newConnection(config);	
		}
	}
	/*
	 * Creates the SObject analytics request object
	 */
	private SObject getAppAnalyticsRequest(String type) {
		
		SObject appAnalyticsQueryRequest = new SObject();
		appAnalyticsQueryRequest.setType("AppAnalyticsQueryRequest");
		appAnalyticsQueryRequest.setField("DataType",type);
		
		return appAnalyticsQueryRequest;
	}
	
	/*
	 * method polls the Salesforce API with a 10 second sleep, checking for the requested analytics operation. 
	 * If complete, will return the URL where the results file will be downloaded from. 
	 * 
	 * Can return null if complete without a Download URL
	 */
	private String pollForDownloadUrl(String requestId) throws ConnectionException{
		
		String query = String.format("select DownloadUrl, DownloadSize, RequestState from AppAnalyticsQueryRequest where id = '%s'",requestId);
		boolean isDone = false;
		
		while(!isDone) {
			
			QueryResult qr = conn.query(query);
			SObject[] records = qr.getRecords();
			
			if(records.length != 1) {
				System.out.println("RECORDS LENGTH ["+records.length+"]");
				throw new RuntimeException();
			}
			SObject record = records[0];
			if(requestFinished(record)) {
				String downloadSize = (String)record.getField("DownloadSize");
				String downloadUrl = (String)record.getField("DownloadUrl");
				System.out.println(String.format("We have a file size %s bytes to retrieve from %s", downloadSize, downloadUrl));
				return downloadUrl;
			} else {
				System.out.println("Looping until we have the download URL is complete");
			}
			try {
				TimeUnit.SECONDS.sleep(10);
			} catch (InterruptedException ie) {
				ie.printStackTrace();
				throw new RuntimeException(ie);
			}
		}
		return null;
	}
	
	/*
	 * test the SObject and look for the signal that request has completed on server. 
	 */
	private boolean requestFinished(SObject record) {
		String requestState = (String)record.getField("RequestState");
		switch (requestState) {
		case "Complete" :
			return true;
		case "Expired" :
		case "Failed" :
		case "NoData" :
			System.out.println("Something wrong with request ["+requestState+"]");
			return true;

		default:
			return false;
		}
	}
	
	/*
	 * download the file to the specified file name.
	 */
	private void downloadFile(String downloadUrl, String filename) throws MalformedURLException, IOException {
		ReadableByteChannel readableByteChannel = Channels.newChannel(new URL(downloadUrl).openStream());
		FileOutputStream fos = new FileOutputStream(filename);
		FileChannel fileChannel = fos.getChannel();
		fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
		fileChannel.close();
		fos.close(); 
	}

	/*
	 * copys the CSV file specified in the filepath to the table in postgres.
	 */
	protected long copyToPostgres (String filepath, String table) {
		try {
			List<String> lines = Files.readAllLines(Paths.get(filepath));
			String copyQuery = getCopyQuery(lines.get(0), table);
			
			try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))){
				CopyManager copyManager = new CopyManager((BaseConnection)conn);
				CopyIn copyIn = copyManager.copyIn(copyQuery);
				
				for(int i=1 ; i<lines.size() ; i++) {
					String row = lines.get(i);
					if(!row.endsWith("\n")) {
						row += "\n";
					}
					byte[] bytes = row.getBytes();
					copyIn.writeToCopy(bytes, 0, bytes.length);
				}
				long rowsInserted = copyIn.endCopy();
				System.out.printf("%d row(s) inserted %n", rowsInserted);
				return rowsInserted;
			} catch (SQLException sq) {
				sq.printStackTrace();
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return -1;
	}
	
	/*
	 * builds the COPY query for the specified table.
	 * expects column headers to be the same as table colums.
	 */
	protected String getCopyQuery(String line, String table) {
		StringTokenizer stok = new StringTokenizer(line,",");
		StringBuilder builder = new StringBuilder("COPY ");
		builder.append(table);
		builder.append("(");
		while(stok.hasMoreTokens()) {
			String columnHeader = stok.nextToken();
			builder.append(columnHeader);
			if(stok.hasMoreTokens()) {
				builder.append(",");
			} else {
				builder.append(")");
			}
		}
		builder.append(" from STDIN WITH (FORMAT csv, HEADER true);");
		return builder.toString();
	}
	
	/*
	 *  returns a list of distinct org ids from the package summary. 
	 *  pulls from the latest package summary in the database only. 
	 */
	private String[] getOrgIds() {
		
		List<String> orgIds = new ArrayList<>();
		
		try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))){
			PreparedStatement ps = conn.prepareStatement("select distinct(organization_id) from package_usage_summary where month = (select distinct(month) from package_usage_summary order by 1 desc limit 1)");
			ResultSet rs = ps.executeQuery();
			while(rs.next()) {
				orgIds.add(rs.getString(1));
			}
		} catch(SQLException sqle) {
			sqle.printStackTrace();
		}

		return orgIds.toArray(new String[0]);
	}

	// returns true if we can find any records in the package_usage_log table for yesterday.
	private boolean latestPackageUsageExists() {

		try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))) {
			PreparedStatement ps = conn.prepareStatement("select count(*) from package_usage_log where timestamp_derived  >= ? and timestamp_derived < ?");
			Calendar fromCalendar =  Calendar.getInstance(TimeZone.getTimeZone("PST"));
			Calendar toCalendar = Calendar.getInstance(TimeZone.getTimeZone("PST"));
			fromCalendar.add(Calendar.DAY_OF_YEAR, -1);
			ps.setDate(1,new Date(fromCalendar.getTimeInMillis()));
			ps.setDate(2, new Date(toCalendar.getTimeInMillis()));
			ResultSet rs = ps.executeQuery();
			if(rs.next()) {
				if (rs.getLong(1) > 0) {
					return true;
				}
			}
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
		return false;
	}
}
