package com.craiglowery.java.vlib.tuple.filterexp;
/* Author: James Craig Lowery
 *         January, 2016
 *         
 *         This code is a personal endeavor and is part of the
 *         Binary Large Object Store system.
 *         
 */
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SmartDateTimeParser {
	
	static public TupleExpressionFactory.Node exp;

	/**
	 * Converts a number of date strings to Instant.<p>
	 * 
	 * Example formats are:
	 * <ol>
	 *     <li> Jan 7, 2015 3:15pm<p>
	 *     <li> 2015-03-07 3:15pm
	 *     <li> 2015-03-07 15:15
	 *     <li> 3/7/15 3:15pm
	 *     <li> 3/7/15 15:15:30
	 *     <li> 1-Jan-15 3:15pm CDT
	 *     <li> 2015-01-22T06:36:48.149271Z
	 * </ol>
	 * 
	 * In general, this is a three-field  "date time tz"  format where date can 
	 * be any of the above date formats, and the time can be any 
	 * of the above time formats. The tz is optional. If not specified, 
	 * the local time zone is assumed.  If it is specified, then it
	 * must be a string that {@code java.util.time.TimeZone} can parse.<p>
	 * 
	 * The algorithm attempts to identify if only a date or only a time are provided.
	 * If only a date is provided, with no time, then midnight in the local timezone
	 * on the morning of that date is assumed. If only a time is provided, then today's
	 * date is assumed.<p>
	 * 
	 * As a special case, the algorithm recognizes the last case above
	 * which is the java ISO_INSTANT format.
	 * 
	 * @param s The string to be parsed.
	 * @return The {@code Instant} represented by {@code s}, or null if {@code s} could not be parsed. 
	 */
	public static Instant parse(String s) 
			throws NumberFormatException
	{
		try {

			if (s==null) {
				throw new NumberFormatException("Null string cannot be parsed");
			}
			
			//Maybe it is ISO_INSTANT - this is a common case since the
			//system generates such strings in XML to represent instances, and
			//they often get sent back to us.
			try {
				Instant i = Instant.parse(s);
				return i;
			} catch (DateTimeParseException e) {
				//ignore
			}
			
			if (s.toLowerCase().equals("now"))
				return Instant.now();
			
			String s_month=null;
			String s_day=null;
			String s_year=null;
			String s_time=null;
			String s_hour="0";
			String s_minute="0";
			String s_second="0";
			String s_ampm=null;
			String s_tz=null;
			
			boolean assumeToday=false;
		
			Matcher m = p_date1.matcher(s);
			if (m.matches()) {
				s_month=m.group(1).toLowerCase();
				switch (s_month.charAt(0)) {
				case 'j': //jan, jun, jul
					if (s_month.charAt(1)=='a')
						s_month = "1";
					else if (s_month.charAt(2)=='n')
						s_month = "6";
					else
						s_month="7";
					break;
				case 'f': //feb
					s_month="2";
					break;
				case 'a': //apr, aug
					if (s_month.charAt(1)=='p')
						s_month="4";
					else
						s_month="8";
					break;
				case 'm': //may
					s_month="5";
					break;
				case 'o': //oct
					s_month="10";
					break;
				case 'd': //dec
					s_month="12";
					break;
				}
				s_day = m.group(2);
				s_year = m.group(3);  if (s_year==null) s_year = String.valueOf(LocalDateTime.now().getYear());
				s_time = m.group(4);
			} else {
				m=p_date2.matcher(s);
				if (m.matches()) {
					s_year=m.group(1);
					s_month=m.group(2);
					s_day=m.group(3);
					s_time=m.group(4);
				} else {
					m=p_date3.matcher(s);
					if (m.matches()) {
						s_month=m.group(1);
						s_day=m.group(2);
						s_year=m.group(3);
						if (s_year.length()==2)
							s_year = "20"+s_year;
						s_time=m.group(4);
					} else {
						//Maybe it is only a time.
						assumeToday=true;
						s_time=s;
					}
				}
			}
			
			//At this point, we have year, month, and day of month
			//If a time component was provided, we need to parse it
			//Otherwise assume midnight (which is already set by default values)
			

			if  (s_time!=null && s_time.length()>0) {
				if ((m=p_time.matcher(s_time)).matches()) {
					s_hour = m.group(1);
					s_minute = m.group(2);	if (s_minute==null) s_minute="00";
					s_second = m.group(3);  if (s_second==null) s_second="00";
					s_ampm = m.group(4); if (s_ampm!=null) s = s.toLowerCase();
					s_tz = m.group(5);
				} else  
					throw new NumberFormatException("Unable to make a sensible parse");
			}
			
			int year;
			int month;
			int day;
			if (assumeToday) {
				LocalDateTime today = LocalDateTime.now();
				year=today.getYear();
				month=today.getMonthValue();
				day=today.getDayOfMonth();
			} else {
				year =  Integer.decode(s_year);
				month = Integer.decode(s_month);
				day = Integer.decode(s_day);
			}
			int hour = Integer.decode(s_hour);
			int minute = Integer.decode(s_minute);
			int second = Integer.decode(s_second);
			
			//If a meridian was specified and it is post, add 12 to horu
			if (s_ampm!=null && s_ampm.charAt(0)=='p')
				hour += 12;
			else if (hour==12)
				hour=0;
			
			Instant i;
			if (s_tz==null) {
				//No timezone was specified, so we will assume the defaule timezone
				s_tz = TimeZone.getDefault().getID();
			}
			ZonedDateTime zdt = ZonedDateTime.of(year, month, day, hour, minute, second, 0,ZoneId.of(s_tz));
			i=Instant.from(zdt);
			return i;
		} catch (Exception e) {
			throw new NumberFormatException("Parse failure: "+e.getMessage());
		}
					
			
	}
	
	public static Instant tryParse(String in) {
		try {
			return parse(in);
		} catch (Exception e) {
			return null;
		}
	}
	
	static final String date1 = "^\\s*(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\\s*(\\d\\d?)(?:, ?((?:19|20)\\d\\d))?(.*)$"; 	// Mmm dd, yyyy
	static final String date2 = "^\\s*((?:1|2)\\d\\d\\d)-(\\d\\d?)-(\\d\\d?)?(.*)$";													// yyyy-mm-dd
	static final String date3 = "^\\s*([01]?\\d)/([123]?\\d)/((?:19|20)?\\d\\d)?(.*)$";												// mm/dd/[yy]yy
	static final String time  = "^\\s*([012]?\\d)(?::([0-5]\\d)(?::([0-5]\\d))?)? ?([ap]m?)?(?:\\s+([^\\s]*))?\\s*$";					// hh[:mm[:ss]][pm] [tz] 
	static final Pattern p_date1 = Pattern.compile(date1,Pattern.CASE_INSENSITIVE);
	static final Pattern p_date2 = Pattern.compile(date2,Pattern.CASE_INSENSITIVE);
	static final Pattern p_date3 = Pattern.compile(date3,Pattern.CASE_INSENSITIVE);
	static final Pattern p_time = Pattern.compile(time,Pattern.CASE_INSENSITIVE);
	

}
