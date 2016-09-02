/*
 * Copyright 2016 Hurence
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hurence.logisland.utils.time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * Useful Date utilities.
 *
 * @author bailett & BalusC
 * @link http://balusc.omnifaces.org/2007/09/dateutil.html
 */
public final class DateUtil {


    // Init ---------------------------------------------------------------------------------------

    private static final Map<String, String> DATE_FORMAT_REGEXPS = new HashMap<String, String>() {{
        put("^[A-Z,a-z]{3}\\s[A-Z,a-z]{3}\\s\\d{1,2}\\s\\d{1,2}:\\d{1,2}:\\d{1,2}\\s[A-Z,a-z]{3}\\s\\d{4}$", "EEE MMM dd hh:mm:ss zzz yyyy");
        put("^[A-Z,a-z]{3},\\s\\d{1,2}\\s[A-Z,a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{1,2}:\\d{1,2}\\s[A-Z,a-z]{3}$", "EEE, dd MMM yyyy HH:mm:ss z");
        put("^\\d{4}-\\d{2}-\\d{2}[T,t]\\d{2}:\\d{2}:\\d{2}\\.\\d{3}[A-Z,a-z]{3}$","yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        put("^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2},\\d{3}$","yyyy-MM-dd HH:mm:ss,SSS");
        put("^\\d{4}-\\d{2}-\\d{2}[T,t]\\d{2}:\\d{2}:\\d{2}[A-Z,a-z]{3}$","yyyy-MM-dd'T'HH:mm:ssz");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy-MM-dd HH:mm:ss");
        put("^\\d{4}-\\d{2}-\\d{2}[T,t]\\d{2}:\\d{2}:\\d{2}\\.\\d{4}\\+\\d{2}:\\d{2}$", "yyyy-MM-dd'T'HH:mm:ss.SSSSX");
        put("^\\d{8}$", "yyyyMMdd");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}$", "dd-MM-yyyy");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}$", "yyyy-MM-dd");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}$", "MM/dd/yyyy");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}$", "yyyy/MM/dd");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}$", "dd MMM yyyy");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}$", "dd MMMM yyyy");
        put("^\\d{12}$", "yyyyMMddHHmm");
        put("^\\d{8}\\s\\d{4}$", "yyyyMMdd HHmm");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}$", "dd-MM-yyyy HH:mm");
        put("^\\d{4}-\\d{1,2}-\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy-MM-dd HH:mm");
        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}$", "MM/dd/yyyy HH:mm");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}$", "yyyy/MM/dd HH:mm");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMM yyyy HH:mm");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}$", "dd MMMM yyyy HH:mm");
        put("^\\d{14}$", "yyyyMMddHHmmss");
        put("^\\d{8}\\s\\d{6}$", "yyyyMMdd HHmmss");
        put("^\\d{1,2}-\\d{1,2}-\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd-MM-yyyy HH:mm:ss");

        put("^\\d{1,2}/\\d{1,2}/\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "MM/dd/yyyy HH:mm:ss");
        put("^\\d{4}/\\d{1,2}/\\d{1,2}\\s\\d{1,2}:\\d{2}:\\d{2}$", "yyyy/MM/dd HH:mm:ss");
        put("^\\d{1,2}\\s[a-z]{3}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMM yyyy HH:mm:ss");
        put("^\\d{1,2}\\s[a-z]{4,}\\s\\d{4}\\s\\d{1,2}:\\d{2}:\\d{2}$", "dd MMMM yyyy HH:mm:ss");


        put("^\\d{1,2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2}\\s\\+\\d{4}$","dd/MMM/yyyy:HH:mm:ss Z"); //"02/JAN/2014:09:43:49 +0200"

    }};

    private DateUtil() {
        // Utility class, hide the constructor.
    }


	/**
	 * Formats dates to sortable UTC strings in compliance with ISO-8601.
	 *
	 * @author Adam Matan <adam@matan.name>
	 * @see "http://stackoverflow.com/questions/11294307/convert-java-date-to-utc-string/11294308"
	 */
	public static final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS zzz";
	public static final String LEGACY_FORMAT = "EEE MMM dd hh:mm:ss zzz yyyy";
	private static final TimeZone tz = TimeZone.getTimeZone("Europe/Paris");
	private static final SimpleDateFormat legacyFormatter = new SimpleDateFormat(LEGACY_FORMAT, new Locale("en", "US"));
	private static final SimpleDateFormat isoFormatter = new SimpleDateFormat(ISO_FORMAT, new Locale("en", "US"));

	static {
		legacyFormatter.setTimeZone(tz);
		isoFormatter.setTimeZone(tz);
	}
	private static final Logger log = LoggerFactory.getLogger(DateUtil.class);

	public static String getDateNow() {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");
		return sdf.format(date);
	}

	/**
	 * build a list of n latest dates from today
	 * 
	 * @param n nb days from now
	 * @return something like {2014.05.12,2014.05.13} if today is 2014.05.13 and n = 2
	 */
	public static String getLatestDaysFromNow(int n) {
		String dates = "{";
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd");

		DateTime start = new DateTime();

		for (int i = 0; i < n; i++) {
			Date date = start.minusDays(i).toDate();
			dates += sdf.format(date);
			if(i!= n-1){
				dates += ",";
			}else{
				dates += "}";
			}
		}
		
		log.info(dates);

		return dates;
	}


	/**
	 * Compute the week number of parameter date
	 *
	 * @param date
	 * @return Week number : ex 13
	 */
	public static int getWeekNumberFromDate(Date date) {

		if (date != null) {
			Calendar calendar = Calendar.getInstance();
			calendar.setFirstDayOfWeek(Calendar.MONDAY);
			calendar.setTime(date);

			return calendar.get(Calendar.WEEK_OF_YEAR);
		} else {
			return 0;
		}
	}

	/**
	 * Check if the date parameter occurs during a weekend.
	 *
	 * @return true or false
	 */
	public static boolean isWeekend(Date date) {
		if (date != null) {
			Calendar calendar = Calendar.getInstance();
			calendar.setFirstDayOfWeek(Calendar.MONDAY);
			calendar.setTime(date);
			int dayOfTheWeek = calendar.get(Calendar.DAY_OF_WEEK);
			return dayOfTheWeek == Calendar.SATURDAY || dayOfTheWeek == Calendar.SUNDAY;
		} else {
			return false;
		}

	}

	/**
	 * Check if the date parameter is within a given range of hours
	 *
	 * @return true or false
	 */
	public static boolean isWithinHourRange(Date date, int startHour, int stopHour) {
		if (stopHour < startHour) {
			throw new IllegalArgumentException("start hour shall be before stop hour");
		}

		if (date != null) {
			Calendar calendar = Calendar.getInstance();
			calendar.setFirstDayOfWeek(Calendar.MONDAY);
			calendar.setTime(date);
			int currentHour = calendar.get(Calendar.HOUR_OF_DAY);

			return currentHour >= startHour && currentHour <= stopHour;
		} else {
			return false;
		}

	}

	/**
	 * Formats the current time in a sortable ISO-8601 UTC format.
	 *
	 * @return Current time in ISO-8601 format, e.g. : "2012-07-03T07:59:09.206 UTC"
	 */
	public static String now() {
		return DateUtil.toString(new Date());
	}

	/**
	 * Formats a given date in a sortable ISO-8601 UTC format.
	 *
	 * <pre>
	 * <code>
	 * final Calendar moonLandingCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
	 * moonLandingCalendar.set(1969, 7, 20, 20, 18, 0);
	 * final Date moonLandingDate = moonLandingCalendar.getTime();
	 * System.out.println("UTCDate.toString moon:       " + PrettyDate.toString(moonLandingDate));
	 * >>> UTCDate.toString moon:       1969-08-20T20:18:00.209 UTC
	 * </code>
	 * </pre>
	 *
	 * @param date Valid Date object.
	 * @return The given date in ISO-8601 format.
	 *
	 */
	public static String toString(final Date date) {
		return isoFormatter.format(date);
	}

	public static Date fromIsoStringToDate(String isoDateString) throws ParseException {

		return isoFormatter.parse(isoDateString);

	}

	/**
	 * Formats a given date in the standard Java Date.toString(), using UTC instead of locale time zone.
	 *
	 * <pre>
	 * <code>
	 * System.out.println(UTCDate.toLegacyString(new Date()));
	 * >>> "Tue Jul 03 07:33:57 UTC 2012"
	 * </code>
	 * </pre>
	 *
	 * @param date Valid Date object.
	 * @return The given date in Legacy Date.toString() format, e.g. "Tue Jul 03 09:34:17 IDT 2012"
	 */
	public static String toLegacyString(final Date date) {
		return legacyFormatter.format(date);
	}

	public static Date fromLegacyStringToDate(String legacyDateString) throws ParseException {

		return legacyFormatter.parse(legacyDateString);

	}

	/**
	 * Formats a date in any given format at UTC.
	 *
	 * <pre>
	 * <code>
	 * final Calendar moonLandingCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
	 * moonLandingCalendar.set(1969, 7, 20, 20, 17, 40);
	 * final Date moonLandingDate = moonLandingCalendar.getTime();
	 * PrettyDate.toString(moonLandingDate, "yyyy-MM-dd")
	 * >>> "1969-08-20"
	 * </code>
	 * </pre>
	 *
	 *
	 * @param date Valid Date object.
	 * @param format String representation of the format, e.g. "yyyy-MM-dd"
	 * @return The given date formatted in the given format.
	 */
	public static String toString(final Date date, final String format) {
		return toString(date, format, "UTC");
	}

	/**
	 * Formats a date at any given format String, at any given Timezone String.
	 *
	 *
	 * @param date Valid Date object
	 * @param format String representation of the format, e.g. "yyyy-MM-dd HH:mm"
	 * @param timezone String representation of the time zone, e.g. "CST"
	 * @return The formatted date in the given time zone.
	 */
	public static String toString(final Date date, final String format, final String timezone) {
		final TimeZone tz = TimeZone.getTimeZone(timezone);
		final SimpleDateFormat formatter = new SimpleDateFormat(format);
		formatter.setTimeZone(tz);
		return formatter.format(date);
	}

	public static String outputDateInfo(Date d) {
		String output = "";
		Calendar c = GregorianCalendar.getInstance(tz);
		c.setTimeInMillis(d.getTime());
		TimeZone tzCal = c.getTimeZone();

		output += "Date:                         " + d + "\n";                  // toString uses current system TimeZone
		output += "Date Millis:                  " + d.getTime() + "\n";
		output += "Cal Millis:                   " + c.getTimeInMillis() + "\n";
		output += "Cal To Date Millis:           " + c.getTime().getTime() + "\n";
		output += "Cal TimeZone Name:            " + tzCal.getDisplayName() + "\n";
		output += "Cal TimeZone ID:              " + tzCal.getID() + "\n";
		output += "Cal TimeZone DST Name:        " + tzCal.getDisplayName(true, TimeZone.SHORT) + "\n";
		output += "Cal TimeZone Standard Name:   " + tzCal.getDisplayName(false, TimeZone.SHORT) + "\n";
		output += "In DayLight:                  " + tzCal.inDaylightTime(d) + "\n";

		output += "" + "\n";
		output += "Day Of Month:                 " + c.get(Calendar.DAY_OF_MONTH) + "\n";
		output += "Month Of Year:                " + c.get(Calendar.MONTH) + "\n";
		output += "Year:                         " + c.get(Calendar.YEAR) + "\n";

		output += "Hour Of Day:                  " + c.get(Calendar.HOUR_OF_DAY) + "\n";
		output += "Minute:                       " + c.get(Calendar.MINUTE) + "\n";
		output += "Second:                       " + c.get(Calendar.SECOND) + "\n";

		return output;
	}

	//1 minute = 60 seconds
	//1 hour = 60 x 60 = 3600
	//1 day = 3600 x 24 = 86400
	public static String getElapsedTime(Date startDate, Date endDate) {

		//milliseconds
		long different = endDate.getTime() - startDate.getTime();

		long secondsInMilli = 1000;
		long minutesInMilli = secondsInMilli * 60;
		long hoursInMilli = minutesInMilli * 60;
		long daysInMilli = hoursInMilli * 24;

		long elapsedDays = different / daysInMilli;
		different = different % daysInMilli;

		long elapsedHours = different / hoursInMilli;
		different = different % hoursInMilli;

		long elapsedMinutes = different / minutesInMilli;
		different = different % minutesInMilli;

		long elapsedSeconds = different / secondsInMilli;

		return String.format(
			"%d ms (%d days, %d hours, %d minutes, %d seconds)",
			different, elapsedDays, elapsedHours, elapsedMinutes, elapsedSeconds);

	}

	/**
	 * check if a duration has expired
	 *
	 * @param fromDate
	 * @param timeout
	 * @return
	 */
	public static boolean isTimeoutExpired(Date fromDate, long timeout) {
		Date now = new Date();
		long elapsedDuration = now.getTime() - fromDate.getTime();

		return (timeout - elapsedDuration) < 0;
	}







	// Converters ---------------------------------------------------------------------------------

	/**
	 * Convert the given date to a Calendar object. The TimeZone will be derived from the local
	 * operating system's timezone.
	 * @param date The date to be converted to Calendar.
	 * @return The Calendar object set to the given date and using the local timezone.
	 */
	public static Calendar toCalendar(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.clear();
		calendar.setTime(date);
		return calendar;
	}

	/**
	 * Convert the given date to a Calendar object with the given timezone.
	 * @param date The date to be converted to Calendar.
	 * @param timeZone The timezone to be set in the Calendar.
	 * @return The Calendar object set to the given date and timezone.
	 */
	public static Calendar toCalendar(Date date, TimeZone timeZone) {
		Calendar calendar = toCalendar(date);
		calendar.setTimeZone(timeZone);
		return calendar;
	}

	/**
	 * Parse the given date string to date object and return a date instance based on the given
	 * date string. This makes use of the {@link DateUtil#determineDateFormat(String)} to determine
	 * the SimpleDateFormat pattern to be used for parsing.
	 * @param dateString The date string to be parsed to date object.
	 * @return The parsed date object.
	 * @throws ParseException If the date format pattern of the given date string is unknown, or if
	 * the given date string or its actual date is invalid based on the date format pattern.
	 */
	public static Date parse(String dateString) throws ParseException {
		String dateFormat = determineDateFormat(dateString);
		if (dateFormat == null) {
			throw new ParseException("Unknown date format for date \'" + dateString + "\'", 0);
		}
		return parse(dateString, dateFormat);
	}

	/**
	 * Validate the actual date of the given date string based on the given date format pattern and
	 * return a date instance based on the given date string.
	 * @param dateString The date string.
	 * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
	 * @return The parsed date object.
	 * @throws ParseException If the given date string or its actual date is invalid based on the
	 * given date format pattern.
	 * @see SimpleDateFormat
	 */
	public static Date parse(String dateString, String dateFormat) throws ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		simpleDateFormat.setLenient(false); // Don't automatically convert invalid date.
		return simpleDateFormat.parse(dateString);
	}

	// Validators ---------------------------------------------------------------------------------

	/**
	 * Checks whether the actual date of the given date string is valid. This makes use of the
	 * {@link DateUtil#determineDateFormat(String)} to determine the SimpleDateFormat pattern to be
	 * used for parsing.
	 * @param dateString The date string.
	 * @return True if the actual date of the given date string is valid.
	 */
	public static boolean isValidDate(String dateString) {
		try {
			parse(dateString);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

	/**
	 * Checks whether the actual date of the given date string is valid based on the given date
	 * format pattern.
	 * @param dateString The date string.
	 * @param dateFormat The date format pattern which should respect the SimpleDateFormat rules.
	 * @return True if the actual date of the given date string is valid based on the given date
	 * format pattern.
	 * @see SimpleDateFormat
	 */
	public static boolean isValidDate(String dateString, String dateFormat) {
		try {
			parse(dateString, dateFormat);
			return true;
		} catch (ParseException e) {
			return false;
		}
	}

	// Checkers -----------------------------------------------------------------------------------

	/**
	 * Determine SimpleDateFormat pattern matching with the given date string. Returns null if
	 * format is unknown. You can simply extend DateUtil with more formats if needed.
	 * @param dateString The date string to determine the SimpleDateFormat pattern for.
	 * @return The matching SimpleDateFormat pattern, or null if format is unknown.
	 * @see SimpleDateFormat
	 */
	public static String determineDateFormat(String dateString) {
		for (String regexp : DATE_FORMAT_REGEXPS.keySet()) {
			if (dateString.toLowerCase().matches(regexp)) {
				return DATE_FORMAT_REGEXPS.get(regexp);
			}
		}
		return null; // Unknown format.
	}

	// Changers -----------------------------------------------------------------------------------

	/**
	 * Add the given amount of years to the given date. It actually converts the date to Calendar
	 * and calls {@link CalendarUtil#addYears(Calendar, int)} and then converts back to date.
	 * @param date The date to add the given amount of years to.
	 * @param years The amount of years to be added to the given date. Negative values are also
	 * allowed, it will just go back in time.
	 */
	public static Date addYears(Date date, int years) {
		Calendar calendar = toCalendar(date);
		CalendarUtil.addYears(calendar, years);
		return calendar.getTime();
	}

	/**
	 * Add the given amount of months to the given date. It actually converts the date to Calendar
	 * and calls {@link CalendarUtil#addMonths(Calendar, int)} and then converts back to date.
	 * @param date The date to add the given amount of months to.
	 * @param months The amount of months to be added to the given date. Negative values are also
	 * allowed, it will just go back in time.
	 */
	public static Date addMonths(Date date, int months) {
		Calendar calendar = toCalendar(date);
		CalendarUtil.addMonths(calendar, months);
		return calendar.getTime();
	}

	/**
	 * Add the given amount of days to the given date. It actually converts the date to Calendar and
	 * calls {@link CalendarUtil#addDays(Calendar, int)} and then converts back to date.
	 * @param date The date to add the given amount of days to.
	 * @param days The amount of days to be added to the given date. Negative values are also
	 * allowed, it will just go back in time.
	 */
	public static Date addDays(Date date, int days) {
		Calendar calendar = toCalendar(date);
		CalendarUtil.addDays(calendar, days);
		return calendar.getTime();
	}

	/**
	 * Add the given amount of hours to the given date. It actually converts the date to Calendar
	 * and calls {@link CalendarUtil#addHours(Calendar, int)} and then converts back to date.
	 * @param date The date to add the given amount of hours to.
	 * @param hours The amount of hours to be added to the given date. Negative values are also
	 * allowed, it will just go back in time.
	 */
	public static Date addHours(Date date, int hours) {
		Calendar calendar = toCalendar(date);
		CalendarUtil.addHours(calendar, hours);
		return calendar.getTime();
	}

	/**
	 * Add the given amount of minutes to the given date. It actually converts the date to Calendar
	 * and calls {@link CalendarUtil#addMinutes(Calendar, int)} and then converts back to date.
	 * @param date The date to add the given amount of minutes to.
	 * @param minutes The amount of minutes to be added to the given date. Negative values are also
	 * allowed, it will just go back in time.
	 */
	public static Date addMinutes(Date date, int minutes) {
		Calendar calendar = toCalendar(date);
		CalendarUtil.addMinutes(calendar, minutes);
		return calendar.getTime();
	}

	/**
	 * Add the given amount of seconds to the given date. It actually converts the date to Calendar
	 * and calls {@link CalendarUtil#addSeconds(Calendar, int)} and then converts back to date.
	 * @param date The date to add the given amount of seconds to.
	 * @param seconds The amount of seconds to be added to the given date. Negative values are also
	 * allowed, it will just go back in time.
	 */
	public static Date addSeconds(Date date, int seconds) {
		Calendar calendar = toCalendar(date);
		CalendarUtil.addSeconds(calendar, seconds);
		return calendar.getTime();
	}

	/**
	 * Add the given amount of millis to the given date. It actually converts the date to Calendar
	 * and calls {@link CalendarUtil#addMillis(Calendar, int)} and then converts back to date.
	 * @param date The date to add the given amount of millis to.
	 * @param millis The amount of millis to be added to the given date. Negative values are also
	 * allowed, it will just go back in time.
	 */
	public static Date addMillis(Date date, int millis) {
		Calendar calendar = toCalendar(date);
		CalendarUtil.addMillis(calendar, millis);
		return calendar.getTime();
	}

	// Comparators --------------------------------------------------------------------------------

	/**
	 * Returns <tt>true</tt> if the two given dates are dated on the same year. It actually
	 * converts the both dates to Calendar and calls
	 * {@link CalendarUtil#sameYear(Calendar, Calendar)}.
	 * @param one The one date.
	 * @param two The other date.
	 * @return True if the two given dates are dated on the same year.
	 * @see CalendarUtil#sameYear(Calendar, Calendar)
	 */
	public static boolean sameYear(Date one, Date two) {
		return CalendarUtil.sameYear(toCalendar(one), toCalendar(two));
	}

	/**
	 * Returns <tt>true</tt> if the two given dates are dated on the same year and month. It
	 * actually converts the both dates to Calendar and calls
	 * {@link CalendarUtil#sameMonth(Calendar, Calendar)}.
	 * @param one The one date.
	 * @param two The other date.
	 * @return True if the two given dates are dated on the same year and month.
	 * @see CalendarUtil#sameMonth(Calendar, Calendar)
	 */
	public static boolean sameMonth(Date one, Date two) {
		return CalendarUtil.sameMonth(toCalendar(one), toCalendar(two));
	}

	/**
	 * Returns <tt>true</tt> if the two given dates are dated on the same year, month and day. It
	 * actually converts the both dates to Calendar and calls
	 * {@link CalendarUtil#sameDay(Calendar, Calendar)}.
	 * @param one The one date.
	 * @param two The other date.
	 * @return True if the two given dates are dated on the same year, month and day.
	 * @see CalendarUtil#sameDay(Calendar, Calendar)
	 */
	public static boolean sameDay(Date one, Date two) {
		return CalendarUtil.sameDay(toCalendar(one), toCalendar(two));
	}

	/**
	 * Returns <tt>true</tt> if the two given dates are dated on the same year, month, day and
	 * hour. It actually converts the both dates to Calendar and calls
	 * {@link CalendarUtil#sameHour(Calendar, Calendar)}.
	 * @param one The one date.
	 * @param two The other date.
	 * @return True if the two given dates are dated on the same year, month, day and hour.
	 * @see CalendarUtil#sameHour(Calendar, Calendar)
	 */
	public static boolean sameHour(Date one, Date two) {
		return CalendarUtil.sameHour(toCalendar(one), toCalendar(two));
	}

	/**
	 * Returns <tt>true</tt> if the two given dates are dated on the same year, month, day, hour
	 * and minute. It actually converts the both dates to Calendar and calls
	 * {@link CalendarUtil#sameMinute(Calendar, Calendar)}.
	 * @param one The one date.
	 * @param two The other date.
	 * @return True if the two given dates are dated on the same year, month, day, hour and minute.
	 * @see CalendarUtil#sameMinute(Calendar, Calendar)
	 */
	public static boolean sameMinute(Date one, Date two) {
		return CalendarUtil.sameMinute(toCalendar(one), toCalendar(two));
	}

	/**
	 * Returns <tt>true</tt> if the two given dates are dated on the same year, month, day, hour,
	 * minute and second. It actually converts the both dates to Calendar and calls
	 * {@link CalendarUtil#sameSecond(Calendar, Calendar)}.
	 * @param one The one date.
	 * @param two The other date.
	 * @return True if the two given dates are dated on the same year, month, day, hour, minute and
	 * second.
	 * @see CalendarUtil#sameSecond(Calendar, Calendar)
	 */
	public static boolean sameSecond(Date one, Date two) {
		return CalendarUtil.sameSecond(toCalendar(one), toCalendar(two));
	}

	// Calculators --------------------------------------------------------------------------------

	/**
	 * Retrieve the amount of elapsed years between the two given dates. It actually converts the
	 * both dates to Calendar and calls {@link CalendarUtil#elapsedYears(Calendar, Calendar)}.
	 * @param before The first date with expected date before the second date.
	 * @param after The second date with expected date after the first date.
	 * @return The amount of elapsed years between the two given dates
	 * @throws IllegalArgumentException If the first date is dated after the second date.
	 * @see CalendarUtil#elapsedYears(Calendar, Calendar)
	 */
	public static int elapsedYears(Date before, Date after) {
		return CalendarUtil.elapsedYears(toCalendar(before), toCalendar(after));
	}

	/**
	 * Retrieve the amount of elapsed months between the two given dates. It actually converts the
	 * both dates to Calendar and calls {@link CalendarUtil#elapsedMonths(Calendar, Calendar)}.
	 * @param before The first date with expected date before the second date.
	 * @param after The second date with expected date after the first date.
	 * @return The amount of elapsed months between the two given dates.
	 * @throws IllegalArgumentException If the first date is dated after the second date.
	 * @see CalendarUtil#elapsedMonths(Calendar, Calendar)
	 */
	public static int elapsedMonths(Date before, Date after) {
		return CalendarUtil.elapsedMonths(toCalendar(before), toCalendar(after));
	}

	/**
	 * Retrieve the amount of elapsed days between the two given dates. It actually converts the
	 * both dates to Calendar and calls {@link CalendarUtil#elapsedDays(Calendar, Calendar)}.
	 * @param before The first date with expected date before the second date.
	 * @param after The second date with expected date after the first date.
	 * @return The amount of elapsed days between the two given dates.
	 * @throws IllegalArgumentException If the first date is dated after the second date.
	 * @see CalendarUtil#elapsedDays(Calendar, Calendar)
	 */
	public static int elapsedDays(Date before, Date after) {
		return CalendarUtil.elapsedDays(toCalendar(before), toCalendar(after));
	}

	/**
	 * Retrieve the amount of elapsed hours between the two given dates. It actually converts the
	 * both dates to Calendar and calls {@link CalendarUtil#elapsedHours(Calendar, Calendar)}.
	 * @param before The first date with expected date before the second date.
	 * @param after The second date with expected date after the first date.
	 * @return The amount of elapsed hours between the two given dates.
	 * @throws IllegalArgumentException If the first date is dated after the second date.
	 * @see CalendarUtil#elapsedHours(Calendar, Calendar)
	 */
	public static int elapsedHours(Date before, Date after) {
		return CalendarUtil.elapsedHours(toCalendar(before), toCalendar(after));
	}

	/**
	 * Retrieve the amount of elapsed minutes between the two given dates. It actually converts the
	 * both dates to Calendar and calls {@link CalendarUtil#elapsedMinutes(Calendar, Calendar)}.
	 * @param before The first date with expected date before the second date.
	 * @param after The second date with expected date after the first date.
	 * @return The amount of elapsed minutes between the two given dates.
	 * @throws IllegalArgumentException If the first date is dated after the second date.
	 * @see CalendarUtil#elapsedMinutes(Calendar, Calendar)
	 */
	public static int elapsedMinutes(Date before, Date after) {
		return CalendarUtil.elapsedMinutes(toCalendar(before), toCalendar(after));
	}

	/**
	 * Retrieve the amount of elapsed seconds between the two given dates. It actually converts the
	 * both dates to Calendar and calls {@link CalendarUtil#elapsedSeconds(Calendar, Calendar)}.
	 * @param before The first date with expected date before the second date.
	 * @param after The second date with expected date after the first date.
	 * @return The amount of elapsed seconds between the two given dates.
	 * @throws IllegalArgumentException If the first date is dated after the second date.
	 * @see CalendarUtil#elapsedSeconds(Calendar, Calendar)
	 */
	public static int elapsedSeconds(Date before, Date after) {
		return CalendarUtil.elapsedSeconds(toCalendar(before), toCalendar(after));
	}

	/**
	 * Retrieve the amount of elapsed milliseconds between the two given dates. It actually converts
	 * the both dates to Calendar and calls {@link CalendarUtil#elapsedMillis(Calendar, Calendar)}.
	 * @param before The first date with expected date before the second date.
	 * @param after The second date with expected date after the first date.
	 * @return The amount of elapsed milliseconds between the two given dates.
	 * @throws IllegalArgumentException If the first date is dated after the second date.
	 * @see CalendarUtil#elapsedMillis(Calendar, Calendar)
	 */
	public static long elapsedMillis(Date before, Date after) {
		return CalendarUtil.elapsedMillis(toCalendar(before), toCalendar(after));
	}

	/**
	 * Calculate the total of elapsed time from years up to seconds between the two given dates. It
	 * Returns an int array with the elapsed years, months, days, hours, minutes and seconds
	 * respectively. It actually converts the both dates to Calendar and calls
	 * {@link CalendarUtil#elapsedTime(Calendar, Calendar)}.
	 * @param before The first date with expected date before the second date.
	 * @param after The second date with expected date after the first date.
	 * @return The elapsed time between the two given dates in years, months, days, hours, minutes
	 * and seconds.
	 * @throws IllegalArgumentException If the first date is dated after the second date.
	 * @see CalendarUtil#elapsedTime(Calendar, Calendar)
	 */
	public static int[] elapsedTime(Date before, Date after) {
		return CalendarUtil.elapsedTime(toCalendar(before), toCalendar(after));
	}

}