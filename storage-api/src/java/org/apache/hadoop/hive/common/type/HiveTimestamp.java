/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.common.type;

import org.apache.commons.math3.util.Pair;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.NoSuchElementException;
import java.util.TimeZone;

/**
 * A thin wrapper of java.sql.Timestamp, with timezoneID offset.
 * Any timestamp that requires a specific timezone should use this type.
 */
public class HiveTimestamp extends Timestamp {
  private static final ThreadLocal<DateFormat> threadLocalDateFormat =
      new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue() {
          return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
      };

  // We store the offset from UTC in minutes . Ranges from [-12:00, 14:00].
  private Integer offsetInMin = null;

  private String internalID = null;

  private static final int MAX_OFFSET = 840;
  private static final int MIN_OFFSET = -720;

  // Used to indicate no offset is present
  public static final int NULL_OFFSET = -800;

  public HiveTimestamp(long time, String timezoneID) {
    super(time);
    computeOffset(timezoneID);
  }

  public HiveTimestamp(long time) {
    this(time, null);
  }

  public HiveTimestamp(Timestamp other) {
    this(other.getTime());
    setNanos(other.getNanos());
    if (other instanceof HiveTimestamp) {
      this.offsetInMin = ((HiveTimestamp) other).offsetInMin;
    }
  }

  private void computeOffset(String timezoneID) {
    timezoneID = validateTimezoneID(timezoneID);
    if (timezoneID != null) {
      TimeZone tz = TimeZone.getTimeZone(timezoneID);
      offsetInMin = tz.getOffset(getTime()) / 1000 / 60;
    }
  }

  public Integer getOffsetInMin() {
    return offsetInMin;
  }

  public void setOffsetInMin(Integer offsetInMin) {
    validateOffset(offsetInMin);
    this.offsetInMin = offsetInMin;
    internalID = null;
  }

  public boolean hasTimezone() {
    return offsetInMin != null;
  }

  private String getTimezoneID() {
    if (!hasTimezone()) {
      throw new NoSuchElementException("No timezone specified.");
    }
    if (internalID == null) {
      StringBuilder builder = new StringBuilder("GMT");
      if (offsetInMin != 0) {
        if (offsetInMin > 0) {
          builder.append("+");
        } else {
          builder.append("-");
        }
        int tmp = offsetInMin > 0 ? offsetInMin : -offsetInMin;
        int offsetHour = tmp / 60;
        int offsetMin = tmp % 60;
        builder.append(String.format("%02d", offsetHour)).append(":").
            append(String.format("%02d", offsetMin));
      }
      internalID = builder.toString();
    }
    return internalID;
  }

  private static String validateTimezoneID(String timezoneID) {
    if (timezoneID == null) {
      return null;
    }
    TimeZone tz = TimeZone.getTimeZone(timezoneID);
    // We may end up with GMT in case of invalid timezoneID
    if (tz.getID().equals("GMT") && !tz.getID().equals(timezoneID)) {
      throw new IllegalArgumentException("Unknown timezoneID: " + timezoneID);
    }
    return timezoneID;
  }

  @Override
  public String toString() {
    String ts = super.toString();
    if (!hasTimezone()) {
      return ts;
    }
    DateFormat dateFormat = threadLocalDateFormat.get();
    TimeZone defaultTZ = dateFormat.getTimeZone();
    try {
      String timezoneID = getTimezoneID();
      dateFormat.setTimeZone(TimeZone.getTimeZone(timezoneID));
      String r = dateFormat.format(this) + ts.substring(19);
      r += " " + timezoneID;
      return r;
    } finally {
      dateFormat.setTimeZone(defaultTZ);
    }
  }

  public static HiveTimestamp valueOf(String timestamp) {
    Pair<String, String> pair = extractTimezoneID(timestamp);
    return valueOf(pair.getFirst(), pair.getSecond());
  }

  public static HiveTimestamp valueOf(String timestamp, String timezoneID) {
    Timestamp ts = Timestamp.valueOf(timestamp);
    timezoneID = validateTimezoneID(timezoneID);
    if (timezoneID == null) {
      return new HiveTimestamp(ts);
    }
    DateFormat dateFormat = threadLocalDateFormat.get();
    TimeZone defaultTZ = dateFormat.getTimeZone();
    try {
      int nanos = ts.getNanos();
      dateFormat.setTimeZone(TimeZone.getTimeZone(timezoneID));
      Date date = dateFormat.parse(timestamp);
      HiveTimestamp hiveTimestamp = new HiveTimestamp(date.getTime(), timezoneID);
      hiveTimestamp.setNanos(nanos);
      return hiveTimestamp;
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    } finally {
      dateFormat.setTimeZone(defaultTZ);
    }
  }

  // parse s into a timestamp with a timezoneID
  private static Pair<String, String> extractTimezoneID(String s) {
    s = s.trim();
    int divide = s.indexOf(' ');
    if (divide != -1) {
      divide = s.indexOf(' ', divide + 1);
      if (divide != -1) {
        return new Pair<>(s.substring(0, divide), s.substring(divide + 1));
      }
    }
    return new Pair<>(s, null);
  }

  public static boolean isValidOffset(int offsetInMin) {
    return offsetInMin >= MIN_OFFSET && offsetInMin <= MAX_OFFSET;
  }

  private static void validateOffset(Integer offsetInMin) {
    if (offsetInMin != null && !isValidOffset(offsetInMin)) {
      throw new IllegalArgumentException("Timezone offset out of range: " + offsetInMin);
    }
  }
}
