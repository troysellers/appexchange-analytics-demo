package com.grax.aus;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.StringTokenizer;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Hello world!
 *
 */
public class PostgresUtils 
{
	private Dotenv dotenv = Dotenv.load();
	
    public static void main( String[] args ) {
    	
    	PostgresUtils a = new PostgresUtils();
    	try {
    		a.copySubscriberSnapshotToPostgres("subscriber_snapshot-1609719350697.csv");
    		a.copyPackageSummaryToPostgres("package_usage_summary-1609718378717.csv");
    		a.copyPackageUsageLogToPostgres("package_usage_log-1609724908999.csv");
    	} catch (Exception e) {
    		e.printStackTrace();
    		System.exit(1);
    	}
    }
    
    /**
     * Takes a CSV file that has been downloaded from a PackageUsageLog request to the Salesforce API. 
     * Will build the PG Copy and insert the data into the PackageUsageLog table name is defined in  @see com.grax.aus.AnalyticsRequestType.PACKAGE_USAGE_LOG
     * 
     * @param filePath
     * @throws IOException, SQLException, IllegalArgumentException
     */
    public void copyPackageUsageLogToPostgres(String filePath) throws IOException, SQLException, IllegalArgumentException {
    	if(!filePath.endsWith(".csv")) {
    		throw new IllegalArgumentException("PackageLogFile file path should reference a CSV file. Path given is "+filePath);
    	} else {
    		
    		BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
    		
    		@SuppressWarnings("unused")
    		String header = reader.readLine();
    		
    		String firstLineData = reader.readLine();
    		String timestampDerived = firstLineData.split(",")[0];
    		
    		Instant i = Instant.parse(timestampDerived);
    		
    		if(!timestampsExistForDay(i)) {
    			copyToPostgres(filePath);
    		}
    		
    		reader.close();
    	}
    }
    public void copySubscriberSnapshotToPostgres(String filePath) throws IOException, SQLException, IllegalArgumentException {
    
    	if(!filePath.endsWith(".csv")) {
    		throw new IllegalArgumentException("SubscriberSnapshot file path should reference a CSV file. Path given is "+filePath);
    	} else {
    		
    		BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
    		
    		@SuppressWarnings("unused") // burn this first line
    		String header = reader.readLine();
    		
    		String firstDataLine = reader.readLine(); // we want to check the date of this file
    		reader.close();
    		
    		String day = firstDataLine.split(",")[0];
    		if(!dayExists(day)) {
    			copyToPostgres(filePath);
    		}
    	}
    	
    }
    public void copyPackageSummaryToPostgres(String filePath) throws IllegalArgumentException, IOException, SQLException {
    	
    	// validate CSV
    	if(!filePath.endsWith(".csv")) {
    		throw new IllegalArgumentException("PackageSummary file path should reference a CSV file. Path given is "+filePath);
    	} else {
    		
    		// use a buffered reader so we aren't processing the whole file
    		BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
    		
    		@SuppressWarnings("unused")
			String header = reader.readLine(); // file will have a header we don't need for this
    		
    		String firstDataLine = reader.readLine(); // get the first line of actual data
    		reader.close(); // close the reader
    		
    		// month is the first column
    		String[] data = firstDataLine.split(",");
    		String month = data[0];
    		
    		// if we don't have package usage data for this month, insert the CSV using the copy function.
    		if(!monthExists(month)) {
    			copyToPostgres(filePath);
    		}
    	}    	
    }
    /*
     * Builds the copy query from the given CSV file. 
     * Assumes the database table exists.
     * 
     * @param filePath
     * @throws IOException
     */
    private void copyToPostgres(String filePath) throws IOException, SQLException {

    	BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
    	String copyQuery = getCopyQuery(reader.readLine(), filePath.split("\\.")[0]);
    	
		try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))){
			CopyManager copyManager = new CopyManager((BaseConnection)conn);
			CopyIn copyIn = copyManager.copyIn(copyQuery);
			
			String row = "";
			while((row = reader.readLine()) != null) {
				if(!row.endsWith("\n")) {
					row += "\n";
				}
				byte[] bytes = row.getBytes();
				copyIn.writeToCopy(bytes, 0, bytes.length);
			}
			long rowsInserted = copyIn.endCopy();
			System.out.printf("%d row(s) inserted %n", rowsInserted);
			renameFile(filePath);
		}	
    }
    /*
     * builds the PG Copy query from the header that is downloaded from Salesforce API. 
     * 
     * The return can be passed to PG Copy. 
     */
	private String getCopyQuery(String line, String table) {
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
	 * Renames the file once processed by prepending the system time we completed. 
	 */
    private void renameFile(String filePath) {
    	File f = new File(filePath);
    	f.renameTo(new File(System.currentTimeMillis()+"."+filePath));
    }
    
    /*
     * Checks if there any records in the package_usage_summary table for this month. 
     * Returns true if a non-zero is returned from a count(*) query. 
     */
    private boolean monthExists(String month) throws SQLException {
    	
    	System.out.format("Testing if month [%s] exists  in the database table %s\n", month, AnalyticsRequestType.PACKAGE_USAGE_SUMMARY.getDatabaseTableName() );
    	try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))) {
    		PreparedStatement ps = conn.prepareStatement("select count(*) from "+AnalyticsRequestType.PACKAGE_USAGE_SUMMARY.getDatabaseTableName()+" where month = ?");
    		ps.setString(1, month);
    		ResultSet rs = ps.executeQuery();
    		if(rs.next()) {
    			int count = rs.getInt(1);
    			System.out.format("Month exists - %s\n", count != 0);
    			return count != 0;
    		}
    	}
    	
    	return false;
    }
    
    /*
     * Checks if any records in the subscriber snapshot table exist for this day.
     */
    private boolean dayExists(String day) throws SQLException{
    	
    	System.out.format("Testing if day [%s] exists for table [%s]\n", day, AnalyticsRequestType.SUBSCRIBER_SNAPSHOT.getDatabaseTableName());
    	
    	try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))) {
    		PreparedStatement ps = conn.prepareStatement("select count(*) from "+AnalyticsRequestType.SUBSCRIBER_SNAPSHOT.getDatabaseTableName()+" where date = ?");
    		ps.setString(1, day);
    		ResultSet rs = ps.executeQuery();
    		if(rs.next()) {
    			int count = rs.getInt(1);
    			System.out.format("Day exists -%s\n",  count != 0);
    			return count != 0;
    		}
    	} 
    	
    	return false;
    }
    
    /*
     * check if we have records in the package_usage_log table for the particular day
     * 
     */
    private boolean timestampsExistForDay(Instant instant) throws SQLException {
    	System.out.format("Testing if we have records in %s table for the day %s \n", AnalyticsRequestType.PACKAGE_USAGE_LOG.getDatabaseTableName(), instant.toString());
    	
    	try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))) {
    		PreparedStatement ps = conn.prepareStatement("select count(*) from "+AnalyticsRequestType.PACKAGE_USAGE_LOG.getDatabaseTableName()+" where date(timestamp_derived) = ?");
    		ps.setDate(1, new Date(instant.toEpochMilli()));
    		ResultSet rs = ps.executeQuery();
    		if(rs.next()) {
    			int count = rs.getInt(1);
    			System.out.format("We have %d records for day %s \n", count, instant.toString());
    			return count != 0;
    		}
    	} 
    	
    	return false;
    }
}
