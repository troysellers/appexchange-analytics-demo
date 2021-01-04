package com.grax.aus;

public enum AnalyticsRequestType {
	SUBSCRIBER_SNAPSHOT ("SubscriberSnapshot", "subscriber_snapshot.csv", "subscriber_snapshot"), 
	PACKAGE_USAGE_SUMMARY ("PackageUsageSummary", "package_usage_summary.csv", "package_usage_summary"), 
	PACKAGE_USAGE_LOG ("PackageUsageLog", "package_usage_log.csv", "package_usage_log");
	
	private String value;
	private String fileName;
	private String databaseTableName;
	
	AnalyticsRequestType(final String value, final String fileName, final String databaseTableName) {
		this.value = value;
		this.fileName = fileName;
		this.databaseTableName = databaseTableName;
	}
	
	public String getFileName() {
		return fileName;
	}
	public String getValue() {
		return value;
	}
	public String getDatabaseTableName() {
		return databaseTableName;
	}
	
}
