package com.grax.aus;

import static org.junit.Assert.assertEquals;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;

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
    }
    @Test
    public void testCalendar() {
    	Calendar endTime = Calendar.getInstance();
    	endTime.set(Calendar.YEAR, 2021);
    	endTime.set(Calendar.MONTH, Calendar.JANUARY);
    	endTime.set(Calendar.DAY_OF_MONTH, 1);
    	
    	endTime.add(Calendar.DAY_OF_MONTH, -1);
    	
    	assertEquals(endTime.get(Calendar.YEAR), 2020);
    	assertEquals(endTime.get(Calendar.MONTH), Calendar.DECEMBER);
    	assertEquals(endTime.get(Calendar.DAY_OF_MONTH), 31);
    	
    	System.out.println(endTime.getTime());
    	
    }
@Test
    public void testTimestamp() {
    	String iso = "2021-01-02T05:02:56.452Z";
    	
    	Instant i = Instant.parse(iso);

	}
}
	