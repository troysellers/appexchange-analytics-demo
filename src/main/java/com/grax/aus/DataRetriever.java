package com.grax.aus;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.TimeUnit;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import io.github.cdimascio.dotenv.Dotenv;

public class DataRetriever {

	private Dotenv dotenv = Dotenv.load();
	public static void main(String[] args) {
		DataRetriever dr = new DataRetriever();
		try {
			dr.run();
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
	
	public void run() throws ConnectionException, MalformedURLException, IOException {
		ConnectorConfig config = new ConnectorConfig();
		config.setUsername(dotenv.get("SF_USER"));
		config.setPassword(dotenv.get("SF_PASS"));
		config.setPrettyPrintXml(true);
		config.setTraceMessage(true);
		config.setCompression(true);
		config.setAuthEndpoint(dotenv.get("SF_ENDPOINT"));
		
		PartnerConnection pc = Connector.newConnection(config);
		
		String downloadUrl = pollForDownloadUrl("0XI4o000000CaRbGAK", pc);
		if(downloadUrl != null) {
			downloadFile(downloadUrl, "appanalytics.csv");
		}
	}
	
	public String pollForDownloadUrl(String requestId, PartnerConnection pc) throws ConnectionException{
		
		String query = String.format("select DownloadUrl, DownloadSize, RequestState from AppAnalyticsQueryRequest where id = '%s'",requestId);
		boolean isDone = false;
		while(!isDone) {
			QueryResult qr = pc.query(query);
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
	
	public boolean requestFinished(SObject record) {
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
	
	public void downloadFile(String downloadUrl, String filename) throws MalformedURLException, IOException {
		ReadableByteChannel readableByteChannel = Channels.newChannel(new URL(downloadUrl).openStream());
		FileOutputStream fos = new FileOutputStream(filename);
		FileChannel fileChannel = fos.getChannel();
		fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
		fos.close();
	}
}
