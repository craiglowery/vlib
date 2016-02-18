package com.craiglowery.java.vlib.tuple.filterexp;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import static org.junit.Assert.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

/**
 * A simple testing program for the smart date time parser.
 *
 */
public class SmartDateTimeParser_TEST {
	
	private static String nws(Object obj) {
		return obj==null?"(null)":obj.toString();
	}
	
	@Rule
	public ErrorCollector collector = new ErrorCollector();

	public void test(String in, String out, boolean shouldPass) {
		try {
			Instant i = null;
			try {
				i = SmartDateTimeParser.parse(in);
			} catch (Exception e) {
				if (shouldPass)
					fail(nws(in)+e.getMessage());
				return;  //Failure was expected
			}
			if (i==null)
				fail(String.format("%s: Parser returned null instead of throwing exception",nws(in)));
			
			if (!shouldPass) fail(String.format("%s should not have passed parsing", nws(in)));
			String s = i.toString();
			assertTrue(String.format("%s: %s != %s",nws(in),s,nws(out)),s.equals(out));
		} catch (Throwable t) {
			collector.addError(t);
		}
	}
	
	public void testShouldPass(String in, String out) {
		test(in,out,true);
	}
	
	public void testShouldFail(String in, String out) {
		test(in,out,false);
	}

	public static String ISO(int year, int month, int day, int hour, int minute, int second, String tz ) {
		ZonedDateTime zdt = ZonedDateTime.of(year,month,day,hour,minute,second,0,ZoneId.of(tz));
		return zdt.format(DateTimeFormatter.ISO_INSTANT);
	}
	
	
	@Test
	public void testAll() {
		int thisyear = LocalDateTime.now().getYear();
		String localTz = TimeZone.getDefault().getID();
		
		
		testShouldPass("jan 2",ISO(thisyear,1,2,0,0,0,localTz));
		testShouldPass("january 2",ISO(thisyear,1,2,0,0,0,localTz)); 
		testShouldPass("January    2",ISO(thisyear,1,2,0,0,0,localTz));
		testShouldPass("JANUARY 2",ISO(thisyear,1,2,0,0,0,localTz));
		testShouldPass("Feb 14, 1999",ISO(1999,2,14,0,0,0,localTz));
		testShouldPass("Feb 14, 1999 00:00:00",ISO(1999,2,14,0,0,0,localTz));
		testShouldPass("Feb 14, 1999 00:00:00 UTC",ISO(1999,2,14,0,0,0,"UTC"));
		testShouldPass("Feb 14, 1999 12am UTC",ISO(1999,2,14,0,0,0,"UTC"));
		testShouldPass("5/15/15",ISO(2015,5,15,0,0,0,localTz));
		testShouldPass("5/21/90 2:33pm",ISO(2090,5,21,14,33,0,localTz));
		testShouldPass("5/21/2090 2:33:00pm",ISO(2090,5,21,14,33,0,localTz));
		testShouldPass("8/20/15 2pm US/Eastern",ISO(2015,8,20,14,0,0,"US/Eastern"));
		testShouldFail(null,null);
		testShouldFail("FEbRUARY 30",null);
		testShouldFail("2/30/14",null);
		testShouldFail("Februtary 15, 2016 3:15pm",null);
		testShouldFail("21/5/15 5:15:30 US/Central",null);
		testShouldPass("NOW",Instant.now().toString());
		
	}

}
