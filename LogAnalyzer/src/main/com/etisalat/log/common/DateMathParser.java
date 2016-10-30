/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.etisalat.log.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A Simple Utility class for parsing "math" like strings relating to Dates.
 * <p>
 * <p>
 * The basic syntax support addition, subtraction and rounding at various levels
 * of granularity (or "units"). Commands can be chained together and are parsed
 * from left to right. '+' and '-' denote addition and subtraction, while '/'
 * denotes "round". Round requires only a unit, while addition/subtraction
 * require an integer value and a unit. Command strings must not include white
 * space, but the "No-Op" command (empty string) is allowed....
 * </p>
 * <p>
 * <pre>
 *   /HOUR
 *      ... Round to the start of the current hour
 *   /DAY
 *      ... Round to the start of the current day
 *   +2YEARS
 *      ... Exactly two years in the future from now
 *   -1DAY
 *      ... Exactly 1 day prior to now
 *   /DAY+6MONTHS+3DAYS
 *      ... 6 months and 3 days in the future from the start of
 *          the current day
 *   +6MONTHS+3DAYS/DAY
 *      ... 6 months and 3 days in the future from now, rounded
 *          down to nearest day
 * </pre>
 * <p>
 * <p>
 * (Multiple aliases exist for the various units of time (ie:
 * <code>MINUTE</code> and <code>MINUTES</code>; <code>MILLI</code>,
 * <code>MILLIS</code>, <code>MILLISECOND</code>, and <code>MILLISECONDS</code>
 * .) The complete list can be found by inspecting the keySet of
 * {@link #CALENDAR_UNITS})
 * </p>
 * <p>
 * <p>
 * All commands are relative to a "now" which is fixed in an instance of
 * DateMathParser such that
 * <code>p.parseMath("+0MILLISECOND").equals(p.parseMath("+0MILLISECOND"))</code>
 * no matter how many wall clock milliseconds elapse between the two distinct
 * calls to parse (Assuming no other thread calls "<code>setNow</code>" in the
 * interim). The default value of 'now' is the time at the moment the
 * <code>DateMathParser</code> instance is constructed, unless overridden by the
 * </p>
 * <p>
 * <p>
 * All commands are also affected to the rules of a specified {@link TimeZone}
 * (including the start/end of DST if any) which determine when each arbitrary
 * day starts. This not only impacts rounding/adding of DAYs, but also cascades
 * to rounding of HOUR, MIN, MONTH, YEAR as well. The default
 * <code>TimeZone</code> used is <code>UTC</code> unless overridden by the
 */
public class DateMathParser {
    public static final String localTimePatten = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    public static final String localDatePatten = "yyyyMMdd";
    public static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    public static final SimpleDateFormat LOCAL_FORMATER = new SimpleDateFormat(DateMathParser.localTimePatten);
    public static final SimpleDateFormat LOCAL_TIME_FORMATER = new SimpleDateFormat(DateMathParser.localTimePatten);
    public static final SimpleDateFormat LOCAL_DATE_FORMATER = new SimpleDateFormat(DateMathParser.localDatePatten);
    /**
     * Default TimeZone for DateMath rounding (UTC)
     */
    public static final TimeZone DEFAULT_MATH_TZ = UTC;
    /**
     * Default Locale for DateMath rounding (Locale.ROOT)
     */
    public static final Locale DEFAULT_MATH_LOCALE = Locale.ROOT;
    /**
     * A mapping from (uppercased) String labels idenyifying time units, to the
     * corresponding Calendar constant used to set/add/roll that unit of
     * measurement.
     * <p>
     * <p>
     * A single logical unit of time might be represented by multiple labels for
     * convenience (ie: <code>DATE==DAY</code>, <code>MILLI==MILLISECOND</code>)
     * </p>
     *
     * @see Calendar
     */
    public static final Map<String, Integer> CALENDAR_UNITS = makeUnitsMap();
    protected static final Logger logger = LoggerFactory.getLogger(DateMathParser.class);
    private static Pattern splitter = Pattern.compile("\\b|(?<=\\d)(?=\\D)");
    private TimeZone zone;
    private Locale loc;
    private Date now;
    /**
     * Default constructor that assumes UTC should be used for rounding unless
     * otherwise specified in the SolrRequestInfo
     *
     * @see #DEFAULT_MATH_LOCALE
     */
    public DateMathParser() {
        this(null, DEFAULT_MATH_LOCALE);

    }
    /**
     * @param tz The TimeZone used for rounding (to determine when hours/days
     *           begin). If null, then this method defaults to the value
     *           dicated by the SolrRequestInfo if it exists -- otherwise it
     *           uses UTC.
     * @param l  The Locale used for rounding (to determine when weeks begin).
     *           If null, then this method defaults to en_US.
     * @see #DEFAULT_MATH_TZ
     * @see #DEFAULT_MATH_LOCALE
     * @see Calendar#getInstance(TimeZone, Locale)
     */
    public DateMathParser(TimeZone tz, Locale l) {
        loc = (null != l) ? l : DEFAULT_MATH_LOCALE;
        if (null == tz) {
            tz = DEFAULT_MATH_TZ;
        }
        zone = (null != tz) ? tz : DEFAULT_MATH_TZ;
    }

    /**
     * @see #CALENDAR_UNITS
     */
    private static Map<String, Integer> makeUnitsMap() {

        // NOTE: consciously choosing not to support WEEK at this time,
        // because of complexity in rounding down to the nearest week
        // arround a month/year boundry.
        // (Not to mention: it's not clear what people would *expect*)
        //
        // If we consider adding some time of "week" support, then
        // we probably need to change "Locale loc" to default to something
        // from a param via SolrRequestInfo as well.

        Map<String, Integer> units = new HashMap<>(13);
        units.put("YEAR", Calendar.YEAR);
        units.put("YEARS", Calendar.YEAR);
        units.put("MONTH", Calendar.MONTH);
        units.put("MONTHS", Calendar.MONTH);
        units.put("DAY", Calendar.DATE);
        units.put("DAYS", Calendar.DATE);
        units.put("DATE", Calendar.DATE);
        units.put("HOUR", Calendar.HOUR_OF_DAY);
        units.put("HOURS", Calendar.HOUR_OF_DAY);
        units.put("MINUTE", Calendar.MINUTE);
        units.put("MINUTES", Calendar.MINUTE);
        units.put("SECOND", Calendar.SECOND);
        units.put("SECONDS", Calendar.SECOND);
        units.put("MILLI", Calendar.MILLISECOND);
        units.put("MILLIS", Calendar.MILLISECOND);
        units.put("MILLISECOND", Calendar.MILLISECOND);
        units.put("MILLISECONDS", Calendar.MILLISECOND);

        return units;
    }

    /**
     * Modifies the specified Calendar by "adding" the specified value of units
     *
     * @throws IllegalArgumentException if unit isn't recognized.
     * @see #CALENDAR_UNITS
     */
    public static void add(Calendar c, int val, String unit) {
        Integer uu = CALENDAR_UNITS.get(unit);
        if (null == uu) {
            throw new IllegalArgumentException("Adding Unit not recognized: " + unit);
        }
        c.add(uu.intValue(), val);
    }

    /**
     * Modifies the specified Calendar by "rounding" down to the specified unit
     *
     * @throws IllegalArgumentException if unit isn't recognized.
     * @see #CALENDAR_UNITS
     */
    public static void round(Calendar c, String unit) {
        Integer uu = CALENDAR_UNITS.get(unit);
        if (null == uu) {
            throw new IllegalArgumentException("Rounding Unit not recognized: " + unit);
        }
        int u = uu.intValue();

        switch (u) {

        case Calendar.YEAR:
            c.clear(Calendar.MONTH);
            /* fall through */
        case Calendar.MONTH:
            c.clear(Calendar.DAY_OF_MONTH);
            c.clear(Calendar.DAY_OF_WEEK);
            c.clear(Calendar.DAY_OF_WEEK_IN_MONTH);
            c.clear(Calendar.DAY_OF_YEAR);
            c.clear(Calendar.WEEK_OF_MONTH);
            c.clear(Calendar.WEEK_OF_YEAR);
            /* fall through */
        case Calendar.DATE:
            c.clear(Calendar.HOUR_OF_DAY);
            c.clear(Calendar.HOUR);
            c.clear(Calendar.AM_PM);
            /* fall through */
        case Calendar.HOUR_OF_DAY:
            c.clear(Calendar.MINUTE);
			/* fall through */
        case Calendar.MINUTE:
            c.clear(Calendar.SECOND);
			/* fall through */
        case Calendar.SECOND:
            c.clear(Calendar.MILLISECOND);
            break;
        default:
            throw new IllegalStateException("No logic for rounding value (" + u + ") " + unit);
        }

    }

    @SuppressWarnings("deprecation")
    public static String utc2Local(String utcTime, String utcTimePatten, String localTimePatten, String timeZone)
            throws ParseException {
        SimpleDateFormat utcFormater = new SimpleDateFormat(utcTimePatten);
        utcFormater.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date gpsUTCDate = null;
        try {
            logger.debug("utcTime : {}", utcTime);

            gpsUTCDate = utcFormater.parse(utcTime);

            //logger.info("Year : " + gpsUTCDate.getYear());
            //logger.info("Month : " + gpsUTCDate.getMonth());
            //logger.info("Day: " + gpsUTCDate.getDay());
            //logger.info("Hour : " + gpsUTCDate.getHours());
            //logger.info("Minute : " + gpsUTCDate.getMinutes());
            logger.debug("utcTime second : {}", gpsUTCDate.getSeconds());
        } catch (ParseException e) {
            throw new ParseException(e.getMessage(), 0);
        }

        SimpleDateFormat localFormater = new SimpleDateFormat(localTimePatten);

        if (null == timeZone) {
            localFormater.setTimeZone(TimeZone.getDefault());
        } else {
            TimeZone tz = TimeZone.getTimeZone(timeZone);
            localFormater.setTimeZone(tz);
        }

        String localTime = localFormater.format(gpsUTCDate.getTime());
        logger.debug("localTime : " + localTime);
        return localTime;
    }

    public static long getSpanDays(String startTime, String endTime) throws ParseException {
        SimpleDateFormat localTimeFormater = new SimpleDateFormat(localTimePatten);
        SimpleDateFormat localDateFormater = new SimpleDateFormat(localDatePatten);

        Date tmStartTime, tmEndTime;
        Date datStartDate, datEndDate;

        long spanDays = 0;
        try {
            tmStartTime = localTimeFormater.parse(startTime);
            tmEndTime = localTimeFormater.parse(endTime);
            //spanDays = (tmEndTime.getTime() - tmStartTime.getTime()) / (24 * 60 * 60 * 1000);
            String strTmpStartDate = localDateFormater.format(tmStartTime);
            String strTmpEndDate = localDateFormater.format(tmEndTime);

            datStartDate = localDateFormater.parse(strTmpStartDate);
            datEndDate = localDateFormater.parse(strTmpEndDate);
            spanDays = (datEndDate.getTime() - datStartDate.getTime()) / (24 * 60 * 60 * 1000);
        } catch (ParseException e) {
            throw new ParseException(e.getMessage(), 0);
        }

        return spanDays;
    }

    public static void main(String[] args) throws Exception {
        SimpleDateFormat localFormater = new SimpleDateFormat(DateMathParser.localTimePatten);
        DateMathParser dateMathParser = new DateMathParser();
        String startTime = localFormater.format(dateMathParser.parseMath("-15MINUTE"));
        String endTime = localFormater.format(dateMathParser.getNow());
        System.out.println("startTime  " + startTime + ", endTime " + endTime);

        long querySpan = DateMathParser.getSpanDays(startTime, endTime);
        System.out.println("spanDays " + querySpan);

        startTime = DateMathParser
                .utc2Local("2016-05-22T09:32:21.000Z", DateMathParser.localTimePatten, DateMathParser.localTimePatten,
                        "GMT+4");
        System.out.println("startTime  " + startTime);
    }

    /**
     * @return the time zone
     */
    public TimeZone getTimeZone() {
        return this.zone;
    }

    /**
     * @return the locale
     */
    public Locale getLocale() {
        return this.loc;
    }

    /**
     * Returns a cloned of this instance's concept of "now".
     * <p>
     * If setNow was never called (or if null was specified) then this method
     * first defines 'now' as the value dictated by the SolrRequestInfo if it
     * exists -- otherwise it uses a new Date instance at the moment getNow() is
     * first called.
     *
     * @see #setNow
     */
    public Date getNow() {
        if (now == null) {
            now = new Date();
        }
        return (Date) now.clone();
    }

    /**
     * Defines this instance's concept of "now".
     *
     * @see #getNow
     */
    public void setNow(Date n) {
        now = n;
    }

    /**
     * Parses a string of commands relative "now" are returns the resulting
     * Date.
     *
     * @throws ParseException positions in ParseExceptions are token positions, not
     *                        character positions.
     */
    public Date parseMath(String math) throws ParseException {

        Calendar cal = Calendar.getInstance(zone, loc);
        cal.setTime(getNow());

		/* check for No-Op */
        if (0 == math.length()) {
            return cal.getTime();
        }

        String[] ops = splitter.split(math);
        int pos = 0;
        while (pos < ops.length) {

            if (1 != ops[pos].length()) {
                throw new ParseException("Multi character command found: \"" + ops[pos] + "\"", pos);
            }
            char command = ops[pos++].charAt(0);

            switch (command) {
            case '/':
                if (ops.length < pos + 1) {
                    throw new ParseException("Need a unit after command: \"" + command + "\"", pos);
                }
                try {
                    round(cal, ops[pos++]);
                } catch (IllegalArgumentException e) {
                    throw new ParseException("Unit not recognized: \"" + ops[pos - 1] + "\"", pos - 1);
                }
                break;
            case '+': /* fall through */
            case '-':
                if (ops.length < pos + 2) {
                    throw new ParseException("Need a value and unit for command: \"" + command + "\"", pos);
                }
                int val = 0;
                try {
                    val = Integer.valueOf(ops[pos++]);
                } catch (NumberFormatException e) {
                    throw new ParseException("Not a Number: \"" + ops[pos - 1] + "\"", pos - 1);
                }
                if ('-' == command) {
                    val = 0 - val;
                }
                try {
                    String unit = ops[pos++];
                    add(cal, val, unit);
                } catch (IllegalArgumentException e) {
                    throw new ParseException("Unit not recognized: \"" + ops[pos - 1] + "\"", pos - 1);
                }
                break;
            default:
                throw new ParseException("Unrecognized command: \"" + command + "\"", pos - 1);
            }
        }

        return cal.getTime();
    }
}