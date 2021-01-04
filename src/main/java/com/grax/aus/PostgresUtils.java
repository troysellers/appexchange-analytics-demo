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
    	} catch (IOException ioe) {
    		ioe.printStackTrace();
    		System.exit(1);
    	}
    }
    
    protected void run() throws IOException {
    	File f = new File("./");
    	for(String s : f.list()) {
    		if (s.endsWith(".csv")) {
    			copyToPostgres(s);
    		}
    	}
	}
    
    public void copyPackageUsageLogToPostgres(String filePath) throws IOException {
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
    public void copySubscriberSnapshotToPostgres(String filePath) throws IOException {
    
    	if(!filePath.endsWith(".csv")) {
    		throw new IllegalArgumentException("SubscriberSnapshot file path should reference a CSV file. Path given is "+filePath);
    	} else {
    		
    		BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
    		
    		@SuppressWarnings("unused") // burn this first line
    		String header = reader.readLine();
    		
    		String firstDataLine = reader.readLine(); // we want to check the date of this file
    		reader.close();
    		
    		String day = firstDataLine.split(",")[0];
    		if(!dayExists(day, "subscriber_snapshot")) {
    			copyToPostgres(filePath);
    		}
    	}
    	
    }
    public void copyPackageSummaryToPostgres(String filePath) throws IllegalArgumentException, IOException{
    	
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
    
    protected void copyToPostgres(String filePath) throws IOException{

    	BufferedReader reader = Files.newBufferedReader(Paths.get(filePath));
    	String copyQuery = getCopyQuery(reader.readLine(), filePath.split("-")[0]);
    	
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
		} catch (SQLException sq) {
			sq.printStackTrace();
			System.exit(1);
		}	
    }
    
	private String getCopyQuery(String line, String table) {
		StringTokenizer stok = new StringTokenizer(line,",");
		StringBuilder builder = new StringBuilder("COPY ");
		builder.append(table);
		builder.append("(");
		while(stok.hasMoreTokens()) {
			String columnHeader = stok.nextToken();
			// fix to get  around the API returning a column header as a reserved SQL word. 
			// TODO : could be much cleaner. 
			/*if(columnHeader.equalsIgnoreCase("date")) {
				builder.append("date_requested");
			} else { */
				builder.append(columnHeader);
			//}
			if(stok.hasMoreTokens()) {
				builder.append(",");
			} else {
				builder.append(")");
			}
		}
		builder.append(" from STDIN WITH (FORMAT csv, HEADER true);");
		return builder.toString();
	}
    private void renameFile(String filePath) {
    	File f = new File(filePath);
    	f.renameTo(new File(filePath+".bak"));
    }
    
    private boolean monthExists(String month) {
    	
    	System.out.format("Testing if month [%s] exists  in the database\n", month);
    	try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))) {
    		PreparedStatement ps = conn.prepareStatement("select count(*) from package_usage_summary where month = ?");
    		ps.setString(1, month);
    		ResultSet rs = ps.executeQuery();
    		if(rs.next()) {
    			int count = rs.getInt(1);
    			System.out.format("Month exists - %s\n", count != 0);
    			return count != 0;
    		}
    	} catch (SQLException sqle) {
    		sqle.printStackTrace();
    		System.exit(1);
    	}
    	return false;
    }
    
    private boolean dayExists(String day, String table) {
    	
    	System.out.format("Testing if day [%s] exists for table [%s]\n", day, table);
    	
    	try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))) {
    		PreparedStatement ps = conn.prepareStatement("select count(*) from "+table+" where date = ?");
    		ps.setString(1, day);
    		ResultSet rs = ps.executeQuery();
    		if(rs.next()) {
    			int count = rs.getInt(1);
    			System.out.format("Day exists -%s\n",  count != 0);
    			return count != 0;
    		}
    	} catch (SQLException sqle) {
    		sqle.printStackTrace();
    		System.exit(1);
    	}
    	
    	return false;
    }
    
    /*
     * check if we have records in the package_usage_log table for the particular day
     * 
     */
    private boolean timestampsExistForDay(Instant instant) {
    	System.out.format("Testing if we have records in package_usage_log for the day %s \n", instant.toString());
    	
    	try (Connection conn = DriverManager.getConnection(dotenv.get("JDBC_DATABASE_URL"))) {
    		PreparedStatement ps = conn.prepareStatement("select count(*) from package_usage_log where date(timestamp_derived) = ?");
    		ps.setDate(1, new Date(instant.toEpochMilli()));
    		ResultSet rs = ps.executeQuery();
    		if(rs.next()) {
    			int count = rs.getInt(1);
    			System.out.format("We have %d records for day %s \n", count, instant.toString());
    			return count != 0;
    		}
    	} catch (SQLException sqle) {
    		sqle.printStackTrace();
    		System.exit(1);
    	}
    	
    	return false;
    }
}
