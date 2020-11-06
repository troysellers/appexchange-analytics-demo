package com.grax.aus;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import io.github.cdimascio.dotenv.Dotenv;

/**
 * Hello world!
 *
 */
public class PostgresCopy 
{
	private Dotenv dotenv = Dotenv.load();
    public static void main( String[] args ) {
    	
    	PostgresCopy a = new PostgresCopy();
    	try {
    		a.run();
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
    
    protected void copyToPostgres(String filePath) throws IOException{
    	AppAnalytics aa = new AppAnalytics();
    	
    	List<String> lines = Files.readAllLines(Paths.get(filePath));
    	String copyQuery = aa.getCopyQuery(lines.get(0), filePath.split("-")[0]);
    	
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
			renameFile(filePath);
		} catch (SQLException sq) {
			sq.printStackTrace();
			System.exit(1);
		}	
    }
    
    private void renameFile(String filePath) {
    	File f = new File(filePath);
    	f.renameTo(new File(filePath+".bak"));
    }
}
