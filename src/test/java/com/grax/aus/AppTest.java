package com.grax.aus;

import static org.junit.Assert.assertEquals;

import org.junit.Test;


/**
 * Unit test for simple App.
 */
public class AppTest 
{
    
    @Test
    public void testEnums() {
		
    	assertEquals("PackageUsageLog",AnalyticsRequestType.PACKAGE_USAGE_LOG.getValue());
    	assertEquals("PackageUsageSummary",AnalyticsRequestType.PACKAGE_USAGE_SUMMARY.getValue());
    	assertEquals("SubscriberSnapshot", AnalyticsRequestType.SUBSCRIBER_SNAPSHOT.getValue());
    	
    	assertEquals("subscriber_snapshot.csv", AnalyticsRequestType.SUBSCRIBER_SNAPSHOT.getFileName());
    	assertEquals("package_usage_summary.csv", AnalyticsRequestType.PACKAGE_USAGE_SUMMARY.getFileName());
    	assertEquals("package_usage_log.csv", AnalyticsRequestType.PACKAGE_USAGE_LOG.getFileName());
    	
    	assertEquals("subscriber_snapshot", AnalyticsRequestType.SUBSCRIBER_SNAPSHOT.getDatabaseTableName());
    	assertEquals("package_usage_summary", AnalyticsRequestType.PACKAGE_USAGE_SUMMARY.getDatabaseTableName());
    	assertEquals("package_usage_log", AnalyticsRequestType.PACKAGE_USAGE_LOG.getDatabaseTableName());
    }

}
	