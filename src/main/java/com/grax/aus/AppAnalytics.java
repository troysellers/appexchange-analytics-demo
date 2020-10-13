package com.grax.aus;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

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
	
	private String packageUsageFile = "packageUsageLog.csv";
	private String packageUsageSummaryFile = "packageUsageSummary.csv";
	private String subscriberSnapshotFile = "subscriberSnapshot.csv";
	
	private String packageUsageLogCopyQuery = "COPY package_usage_log (timestamp_derived, log_record_type, request_id, organization_id, organization_status,"+ 
			"organization_edition, organization_country_code, organization_language_locale,	organization_time_zone, organization_instance," + 
			"user_id_token, user_type, url, package_id, package_version_id, managed_package_namespace, custom_entity, custom_entity_type," + 
			"operation_type, operation_count, request_status, referrer_uri, session_key, login_key, user_agent, user_country_code," + 
			"user_time_zone, api_type, api_version,rows_processed, request_size, response_size, http_method, http_status_code," + 
			"num_fields, app_name, page_app_name, page_context, ui_event_source, ui_event_type, ui_event_sequence_num, target_ui_element," + 
			"parent_ui_element,	page_url, prevpage_url) from STDIN WITH (FORMAT csv, HEADER true);";
	
	private String packageUsageSummaryCopyQuery = "COPY package_usage_summary(date_month, organization_id, package_id, managed_package_namespace," + 
			"custom_entity, custom_entity_type, user_id_token, user_type, num_creates, num_reads, num_updates, num_deletes," + 
			"num_views) from STDIN WITH (FORMAT csv, HEADER true);";
	
	private String subscriberSnapshotCopyQuery = "COPY subscriber_snapshot(date_requested, organization_id, organization_name, organization_status, "
			+ "organization_edition, package_id, package_version, managed_package_namespace, custom_entity, count) "
			+ "from STDIN WITH (FORMAT csv, HEADER true);";
	
	
	private Dotenv dotenv = Dotenv.load();
	
	public static void main(String[] args) {
		AppAnalytics aa = new AppAnalytics();
		try {
			aa.run();
		} catch (Throwable ce) {
			ce.printStackTrace();
		} 
	}

	private void run() throws ConnectionException, MalformedURLException, IOException {

		String line = "";
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		while (line != null) {
			System.out.println("Please enter a choice to load Partner Analytics file:");
			System.out.println("1 - PackageUsageLog");
			System.out.println("2 - PackageUsageSummary");
			System.out.println("3 - SubscriberSnapshot");
			System.out.println("4 - PackageUsageLog with existing");
			System.out.println("5 - PackageUsageSummary with existing");
			System.out.println("6 - SubscriberSnapshot with existing");
			System.out.println("q - Quit");
			System.out.print(":");
			
			line = reader.readLine();
			System.out.println(line);
			
			switch (line) {
			case "1":
				getAnalytics("PackageUsageLog", false);
				break;
			case "2":
				getAnalytics("PackageUsageSummary", false);
				break;
			case "3":
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
	
	private void getAnalytics(String type, boolean useExisting) throws ConnectionException, MalformedURLException, IOException {
		
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(dotenv.get("SF_USER"));
		config.setPassword(dotenv.get("SF_PASS"));
		config.setPrettyPrintXml(true);
		config.setTraceMessage(true);
		config.setCompression(true);
		config.setAuthEndpoint(dotenv.get("SF_ENDPOINT"));
		
		
		SObject appAnalyticsQueryRequest = new SObject();
		appAnalyticsQueryRequest.setType("AppAnalyticsQueryRequest");
		appAnalyticsQueryRequest.setField("DataType", type);
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("PST"));
		String filePath = "";
		String copyQuery = "";
		
		switch (type) {
		case "PackageUsageLog":
			c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH)-2);
			appAnalyticsQueryRequest.setField("StartTime", c);
			filePath = packageUsageFile;
			copyQuery = packageUsageLogCopyQuery;
			break;
		case "PackageUsageSummary":
			c.set(Calendar.MONTH, c.get(Calendar.MONTH)-1);
			appAnalyticsQueryRequest.setField("StartTime", c);
			filePath = packageUsageSummaryFile;
			copyQuery = packageUsageSummaryCopyQuery;
			break;
		case "SubscriberSnapshot":
			c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH)-2);
			appAnalyticsQueryRequest.setField("StartTime", c);
			filePath = subscriberSnapshotFile;
			copyQuery = subscriberSnapshotCopyQuery;
			break;
		default:
			System.out.println(String.format("Invalid type [%s] requested", type));
		}
		
		DataRetriever dr = new DataRetriever();
		if (!useExisting) {
			PartnerConnection pc = Connector.newConnection(config);		
			SaveResult[] results = pc.create(new SObject[] {appAnalyticsQueryRequest});
			
			for(SaveResult sr : results) {
				String recordId = sr.getId();
				if(recordId != null) {
					String downloadUrl = dr.pollForDownloadUrl(recordId, pc);
					if (downloadUrl != null && !downloadUrl.isEmpty()) {
						dr.downloadFile(downloadUrl, filePath);
						loadTableFromFile(copyQuery, filePath);
					}
				} else {
						System.out.println("We have a null record id? ");
				}
			}	
		} else {
			loadTableFromFile(copyQuery, filePath);
		}
	}
	
	
	private void loadTableFromFile(String copyQuery, String filePath) {
		System.out.printf("Attempting to COPY from %s%n",filePath);
		try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))){
			CopyManager copyManager = new CopyManager((BaseConnection)conn);
			CopyIn copyIn = copyManager.copyIn(copyQuery);
			
			List<String> lines = Files.readAllLines(Paths.get(filePath));
			
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
		} catch (SQLException sq) {
			sq.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
