package com.grax.aus;

import java.text.DateFormat;
import java.util.Calendar;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    
    @Test
    public void testTokenizer() {
    	DateFormat df = DateFormat.getDateInstance();
    	Calendar c = Calendar.getInstance();
    	System.out.println(c.get(Calendar.MONTH));
    	System.out.println(df.format(c.getTime()));
    }

}
