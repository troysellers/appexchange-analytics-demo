package com.grax.aus;

public enum AnalyticsRequestType {
	SUBSCRIBER_SNAPSHOT ("SubscriberSnapshot"), 
	PACKAGE_USAGE_SUMMARY ("PackageUsageSummary"), 
	PACKAGE_USAGE_LOG ("PackageUsageLog");
	
	private String value;
	
	AnalyticsRequestType(final String value) {
		this.value = value;
	}
	
	public String getValue() {
		return value;
	}
}
