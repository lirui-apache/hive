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

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * A thin wrapper of java.sql.Timestamp, with timezone ID.
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

  // We need to store a string timezone ID here, instead of an offset because offset may change for
  // the same timezone due to DST.
  private String timezone;

  public HiveTimestamp(long time, String timezone) {
    super(time);
    this.timezone = timezone;
  }

  public HiveTimestamp(long time) {
    this(time, null);
  }

  public HiveTimestamp(Timestamp timestamp, String timezone) {
    this(timestamp.getTime(), timezone);
  }

  public HiveTimestamp(Timestamp other) {
    this(other.getTime());
    if (other instanceof HiveTimestamp) {
      setTimezone(((HiveTimestamp) other).timezone);
    }
  }

  public boolean hasTimezone() {
    return timezone != null && !timezone.isEmpty();
  }

  public String getTimezone() {
    return timezone;
  }

  public void setTimezone(String timezone) {
    this.timezone = timezone;
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
      dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
      String r = dateFormat.format(this) + ts.substring(19);
      r += " " + timezone;
      return r;
    } finally {
      dateFormat.setTimeZone(defaultTZ);
    }
  }

  public static HiveTimestamp valueOf(String timestamp, String timezone) {
    Timestamp ts = Timestamp.valueOf(timestamp);
    if (timezone == null || timezone.isEmpty()) {
      return new HiveTimestamp(ts);
    }
    DateFormat dateFormat = threadLocalDateFormat.get();
    TimeZone defaultTZ = dateFormat.getTimeZone();
    try {
      int nanos = ts.getNanos();
      dateFormat.setTimeZone(TimeZone.getTimeZone(timezone));
      Date date = dateFormat.parse(timestamp);
      HiveTimestamp hiveTimestamp = new HiveTimestamp(date.getTime(), timezone);
      hiveTimestamp.setNanos(nanos);
      return hiveTimestamp;
    } catch (ParseException e) {
      throw new IllegalArgumentException(e);
    } finally {
      dateFormat.setTimeZone(defaultTZ);
    }
  }
}
