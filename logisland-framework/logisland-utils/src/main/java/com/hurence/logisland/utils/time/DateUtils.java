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

public class DateUtils {

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
	private static final Logger log = LoggerFactory.getLogger(DateUtils.class);

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

	public static Date getDateFromLogString(String dateString) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss", new Locale("en", "US"));
		sdf.setTimeZone(tz);

		Date result = null;
		try {
			result = sdf.parse(dateString);
		} catch (ParseException e) {
			log.warn(e.getMessage());
		}
		return result;

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
	 * @return Current time in ISO-8601 format, e.g. : "2012-07-03T07:59:09.206 UTC"
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
		return DateUtils.toString(new Date());
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
}
