/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.util;

import java.sql.Date;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveTimestamp;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hive.common.util.DateUtils;


public class DateTimeMath {

  private static class NanosResult {
    public int seconds;
    public int nanos;

    public void addNanos(int leftNanos, int rightNanos) {
      seconds = 0;
      nanos = leftNanos + rightNanos;
      if (nanos < 0) {
        seconds = -1;
        nanos += DateUtils.NANOS_PER_SEC;
      } else if (nanos >= DateUtils.NANOS_PER_SEC) {
        seconds = 1;
        nanos -= DateUtils.NANOS_PER_SEC;
      }
    }
  }

  protected Calendar calUtc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
  protected Calendar calLocal = Calendar.getInstance();
  protected NanosResult nanosResult = new NanosResult();

  //
  // Operations involving/returning year-month intervals
  //

  /**
   * Perform month arithmetic to millis value using UTC time zone.
   * @param millis
   * @param months
   * @return
   */
  public long addMonthsToMillisUtc(long millis, int months) {
    calUtc.setTimeInMillis(millis);
    calUtc.add(Calendar.MONTH, months);
    return calUtc.getTimeInMillis();
  }

  /**
   * Perform month arithmetic to millis value using local time zone.
   * @param millis
   * @param months
   * @return
   */
  public long addMonthsToMillisLocal(long millis, int months) {
    calLocal.setTimeInMillis(millis);
    calLocal.add(Calendar.MONTH, months);
    return calLocal.getTimeInMillis();
  }

  public long addMonthsToNanosUtc(long nanos, int months) {
    long result = addMonthsToMillisUtc(nanos / 1000000, months) * 1000000 + (nanos % 1000000);
    return result;
  }

  public long addMonthsToNanosLocal(long nanos, int months) {
    long result = addMonthsToMillisLocal(nanos / 1000000, months) * 1000000 + (nanos % 1000000);
    return result;
  }

  public long addMonthsToDays(long days, int months) {
    long millis = DateWritable.daysToMillis((int) days);
    millis = addMonthsToMillisLocal(millis, months);
    // Convert millis result back to days
    return DateWritable.millisToDays(millis);
  }

  public HiveTimestamp add(HiveTimestamp ts, HiveIntervalYearMonth interval) {
    if (ts == null || interval == null) {
      return null;
    }

    HiveTimestamp tsResult = new HiveTimestamp(0);
    add(ts, interval, tsResult);

    return tsResult;
  }

  public boolean add(HiveTimestamp ts, HiveIntervalYearMonth interval, HiveTimestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    // Attempt to match Oracle semantics for timestamp arithmetic,
    // where timestamp arithmetic is done in UTC, then converted back to local timezone
    long resultMillis = addMonthsToMillisUtc(ts.getTime(), interval.getTotalMonths());
    result.setTime(resultMillis);
    result.setNanos(ts.getNanos());

    return true;
  }

  public HiveTimestamp add(HiveIntervalYearMonth interval, HiveTimestamp ts) {
    if (ts == null || interval == null) {
      return null;
    }

    HiveTimestamp tsResult = new HiveTimestamp(0);
    add(interval, ts, tsResult);

    return tsResult;
  }

  public boolean add(HiveIntervalYearMonth interval, HiveTimestamp ts, HiveTimestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    // Attempt to match Oracle semantics for timestamp arithmetic,
    // where timestamp arithmetic is done in UTC, then converted back to local timezone
    long resultMillis = addMonthsToMillisUtc(ts.getTime(), interval.getTotalMonths());
    result.setTime(resultMillis);
    result.setNanos(ts.getNanos());

    return true;
  }

  public Date add(Date dt, HiveIntervalYearMonth interval) {
    if (dt == null || interval == null) {
      return null;
    }

    Date dtResult = new Date(0);
    add(dt, interval, dtResult);

    return dtResult;
  }

  public boolean add(Date dt, HiveIntervalYearMonth interval, Date result) {
    if (dt == null || interval == null) {
      return false;
    }

    // Since Date millis value is in local timezone representation, do date arithmetic
    // using local timezone so the time remains at the start of the day.
    long resultMillis = addMonthsToMillisLocal(dt.getTime(), interval.getTotalMonths());
    result.setTime(resultMillis);
    return true;
  }

  public Date add(HiveIntervalYearMonth interval, Date dt) {
    if (dt == null || interval == null) {
      return null;
    }

    Date dtResult = new Date(0);
    add(interval, dt, dtResult);

    return dtResult;
  }

  public boolean add(HiveIntervalYearMonth interval, Date dt, Date result) {
    if (dt == null || interval == null) {
      return false;
    }

    // Since Date millis value is in local timezone representation, do date arithmetic
    // using local timezone so the time remains at the start of the day.
    long resultMillis = addMonthsToMillisLocal(dt.getTime(), interval.getTotalMonths());
    result.setTime(resultMillis);
    return true;
  }

  public HiveIntervalYearMonth add(HiveIntervalYearMonth left, HiveIntervalYearMonth right) {
    HiveIntervalYearMonth result = null;
    if (left == null || right == null) {
      return null;
    }

    result = new HiveIntervalYearMonth(left.getTotalMonths() + right.getTotalMonths());
    return result;
  }

  public HiveTimestamp subtract(HiveTimestamp left, HiveIntervalYearMonth right) {
    if (left == null || right == null) {
      return null;
    }

    HiveTimestamp tsResult = new HiveTimestamp(0);
    subtract(left, right, tsResult);

    return tsResult;
  }

  public boolean subtract(HiveTimestamp left, HiveIntervalYearMonth right, HiveTimestamp result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  public Date subtract(Date left, HiveIntervalYearMonth right) {
    if (left == null || right == null) {
      return null;
    }

    Date dtResult = new Date(0);
    subtract(left, right, dtResult);

    return dtResult;
  }

  public boolean subtract(Date left, HiveIntervalYearMonth right, Date result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  public HiveIntervalYearMonth subtract(HiveIntervalYearMonth left, HiveIntervalYearMonth right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  //
  // Operations involving/returning day-time intervals
  //

  public HiveTimestamp add(HiveTimestamp ts, HiveIntervalDayTime interval) {
    if (ts == null || interval == null) {
      return null;
    }

    HiveTimestamp tsResult = new HiveTimestamp(0);
    add(ts, interval, tsResult);

    return tsResult;
  }

  public boolean add(HiveTimestamp ts, HiveIntervalDayTime interval,
      HiveTimestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    nanosResult.addNanos(ts.getNanos(), interval.getNanos());

    long newMillis = ts.getTime()
        + TimeUnit.SECONDS.toMillis(interval.getTotalSeconds() + nanosResult.seconds);
    result.setTime(newMillis);
    result.setNanos(nanosResult.nanos);
    return true;
  }

  public HiveTimestamp add(HiveIntervalDayTime interval, HiveTimestamp ts) {
    if (ts == null || interval == null) {
      return null;
    }

    HiveTimestamp tsResult = new HiveTimestamp(0);
    add(interval, ts, tsResult);
    return tsResult;
  }

  public boolean add(HiveIntervalDayTime interval, HiveTimestamp ts,
      HiveTimestamp result) {
    if (ts == null || interval == null) {
      return false;
    }

    nanosResult.addNanos(ts.getNanos(), interval.getNanos());

    long newMillis = ts.getTime()
        + TimeUnit.SECONDS.toMillis(interval.getTotalSeconds() + nanosResult.seconds);
    result.setTime(newMillis);
    result.setNanos(nanosResult.nanos);
    return true;
  }

  public HiveIntervalDayTime add(HiveIntervalDayTime left, HiveIntervalDayTime right) {
    if (left == null || right == null) {
      return null;
    }

    HiveIntervalDayTime result = new HiveIntervalDayTime();
    add(left, right, result);
 
    return result;
  }

  public boolean add(HiveIntervalDayTime left, HiveIntervalDayTime right,
      HiveIntervalDayTime result) {
    if (left == null || right == null) {
      return false;
    }

    nanosResult.addNanos(left.getNanos(), right.getNanos());

    long totalSeconds = left.getTotalSeconds() + right.getTotalSeconds() + nanosResult.seconds;
    result.set(totalSeconds, nanosResult.nanos);
    return true;
  }

  public HiveTimestamp subtract(HiveTimestamp left, HiveIntervalDayTime right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  public boolean subtract(HiveTimestamp left, HiveIntervalDayTime right, HiveTimestamp result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  public HiveIntervalDayTime subtract(HiveIntervalDayTime left, HiveIntervalDayTime right) {
    if (left == null || right == null) {
      return null;
    }
    return add(left, right.negate());
  }

  public boolean subtract(HiveIntervalDayTime left, HiveIntervalDayTime right,
      HiveIntervalDayTime result) {
    if (left == null || right == null) {
      return false;
    }
    return add(left, right.negate(), result);
  }

  public HiveIntervalDayTime subtract(HiveTimestamp left, HiveTimestamp right) {
    if (left == null || right == null) {
      return null;
    }

    HiveIntervalDayTime result = new HiveIntervalDayTime();
    subtract(left, right, result);

    return result;
  }

  public boolean subtract(HiveTimestamp left, HiveTimestamp right,
      HiveIntervalDayTime result) {
    if (left == null || right == null) {
      return false;
    }

    nanosResult.addNanos(left.getNanos(), -(right.getNanos()));

    long totalSeconds = TimeUnit.MILLISECONDS.toSeconds(left.getTime())
        - TimeUnit.MILLISECONDS.toSeconds(right.getTime()) + nanosResult.seconds;
    result.set(totalSeconds, nanosResult.nanos);
    return true;
  }
}
