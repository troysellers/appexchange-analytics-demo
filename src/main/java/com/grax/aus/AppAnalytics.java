package com.grax.aus;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import io.github.cdimascio.dotenv.Dotenv; 

public class AppAnalytics {
	
	protected String packageUsageTable = "package_usage_log";
	protected String packageUsageSummaryTable = "package_usage_summary";
	protected String subscriberSnapshotTable = "subscriber_snapshot";
	
	private Dotenv dotenv = Dotenv.load();
	
	public static void main(String[] args) {
		
		if (args.length != 3) {
			System.out.println("Exiting. You need to pass your SF username, password and security token as paramters");
			System.exit(1);
		}
		
		AppAnalytics aa = new AppAnalytics();
		try {
			aa.run();
		} catch (Throwable ce) {
			ce.printStackTrace();
			System.exit(1);
		} 
	}

	private void run() throws ConnectionException, MalformedURLException, IOException {

		String line = "";
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		while (line != null) {
			System.out.println("Please enter a choice to load Partner Analytics file:");
			System.out.println("1 - PackageUsageLog (last 7 days");
			System.out.println("2 - PackageUsageSummary (last 3 months");
			System.out.println("3 - SubscriberSnapshot (last 7 days)");
			System.out.println("q - Quit");
			System.out.print(":");
			
			line = reader.readLine();
			System.out.println(line);
			
			switch (line) {
			case "1":
				System.out.println("getting package usage logs...");
				getAnalytics("PackageUsageLog", false);
				break;
			case "2":
				System.out.println("getting package summary logs...");
				getAnalytics("PackageUsageSummary", false);
				break;
			case "3":
				System.out.println("getting subscriber summary logs...");
				getAnalytics("SubscriberSnapshot", false);
				break;
			case "4":
				getAnalytics("PackageUsageLog", true);
				break;
			case "5":
				getAnalytics("PackageUsageSummary", true);
				break;
			case "6":
				getAnalytics("SubscriberSnapshot", true);
				break;
			case "q":
			case "Q":
				System.out.println("bye bye!");
				line = null;
				break;
			default:
				System.out.println("Unrecognised input, please choose a valid option from the list. ");
			}
		}
	}
	
	protected void getAnalytics(String type, boolean useExisting) throws  MalformedURLException, IOException {
		
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
			appAnalyticsQueryRequest.setField("DataType", type);
			Calendar c = Calendar.getInstance(TimeZone.getTimeZone("PST"));
			String filePath = "";
			
			switch (type) {
				case "PackageUsageLog":
					
					long daysDifference = 7;
					Calendar c2 = getLatestPackageUsageLogInsert(); // returns the last day we have received this
					c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH)-1); // yesterday should be the latest
					
					if (c2 != null ) {
						// do we need to fetch? 
						daysDifference = daysBetween(c2, c);
						if(daysDifference > 7) {
							daysDifference = 7; //only get the last seven days, we
						} else if (daysDifference <= 1) {
							System.out.println("We have the latest package usage logs already in the database... choose another operation.");
						}
					}
					for(int i=1 ; i<=daysDifference ; i++) {
						
						c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH)-i);
						
						appAnalyticsQueryRequest.setField("StartTime", c);
						filePath = packageUsageTable+"-"+c.getTimeInMillis()+".csv";
						fetchAndWrite(c, appAnalyticsQueryRequest, filePath, pc);
						
					}
					break;
					
				case "PackageUsageSummary":
					for(int i=1 ; i<4 ; i++) {
						c.set(Calendar.MONTH, c.get(Calendar.MONTH)-i);
						appAnalyticsQueryRequest.setField("StartTime", c);
						filePath = packageUsageSummaryTable+"-"+c.getTimeInMillis()+".csv";
						fetchAndWrite(c, appAnalyticsQueryRequest, filePath, pc);
					}
					break;
				case "SubscriberSnapshot":
					Calendar clone = (Calendar)c.clone();
					c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH)-1);
					appAnalyticsQueryRequest.setField("EndTime", c);
					clone.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH)-6);
					appAnalyticsQueryRequest.setField("StartTime", clone);
					filePath = subscriberSnapshotTable+"-"+c.getTimeInMillis()+".csv";
					fetchAndWrite(c, appAnalyticsQueryRequest, filePath, pc);
	
					break;
				default:
					System.out.println(String.format("Invalid type [%s] requested", type));
			}
		} catch (ConnectionException ce) {
			ce.printStackTrace();
			System.exit(1);
		}
	}
	
	protected void fetchAndWrite(Calendar c, SObject appAnalyticsQueryRequest, String filePath, PartnerConnection pc) throws ConnectionException, MalformedURLException, IOException {
		DataRetriever dr = new DataRetriever();

		
		SaveResult[] results = pc.create(new SObject[] {appAnalyticsQueryRequest});
		System.out.println("created a new job for "+appAnalyticsQueryRequest.getType());
		String table = filePath.split("-")[0];
		for(SaveResult sr : results) {
			String recordId = sr.getId();
			
			if(recordId != null) {
				String downloadUrl = dr.pollForDownloadUrl(recordId, pc);
				if (downloadUrl != null && !downloadUrl.isEmpty()) {
					dr.downloadFile(downloadUrl, filePath);
					//copyToPostgres(filePath, table);
				}
			} else {
					System.out.println("We have a null record id? ");
			}
		}	
	}
	
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
				System.exit(1);
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.exit(1);
		}
		return -1;
	}
	protected String getCopyQuery(String line, String table) {
		StringTokenizer stok = new StringTokenizer(line,",");
		StringBuilder builder = new StringBuilder("COPY ");
		builder.append(table);
		builder.append("(");
		while(stok.hasMoreTokens()) {
			String columnHeader = stok.nextToken();
			// fix to get  around the API returning a column header as a reserved SQL word. 
			// TODO : could be much cleaner. 
			if(columnHeader.equalsIgnoreCase("date")) {
				builder.append("date_requested");
			} else {
				builder.append(columnHeader);
			}
			if(stok.hasMoreTokens()) {
				builder.append(",");
			} else {
				builder.append(")");
			}
		}
		builder.append(" from STDIN WITH (FORMAT csv, HEADER true);");
		return builder.toString();
	}

	
	private Calendar getStartCalendarForPackageSummaryLog() {
		
		try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))){
			PreparedStatement ps = conn.prepareStatement("select distinct(date_month) as last_run from package_usage_summary order by 1 desc");
			ResultSet rs = ps.executeQuery();
			rs.next();
			long time = rs.getDate(1).getTime();
			Calendar c = Calendar.getInstance();
			c.setTimeInMillis(time);
			return c;		
		} catch (SQLException sq) {
			sq.printStackTrace();
			System.exit(1);
		}
		return null;
	}
	
	private Calendar getLatestPackageUsageLogInsert() {
		
		try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))){
			PreparedStatement ps = conn.prepareStatement("select max(date_trunc('day',timestamp_derived)) as last_run from package_usage_log");
			ResultSet rs = ps.executeQuery();
			Calendar c = Calendar.getInstance();
			if (rs.getFetchSize() > 0 ) { 
				rs.next();
				long time = rs.getDate(1).getTime();
				c.setTimeInMillis(time);
				return c;
			}
		} catch (SQLException sq) {
			sq.printStackTrace();
			System.exit(1);
		}
		return null;
	}
	
	private long daysBetween(Calendar start, Calendar end) {
		return TimeUnit.MILLISECONDS.toDays(Math.abs(end.getTimeInMillis() - start.getTimeInMillis()));
	}
}
