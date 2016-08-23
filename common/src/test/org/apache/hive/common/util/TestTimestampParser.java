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

package org.apache.hive.common.util;

import static org.junit.Assert.*;

import org.apache.hadoop.hive.common.type.HiveTimestamp;
import org.junit.Test;

public class TestTimestampParser {
  public static class ValidTimestampCase {
    String strValue;
    HiveTimestamp expectedValue;

    public ValidTimestampCase(String strValue, HiveTimestamp expectedValue) {
      this.strValue = strValue;
      this.expectedValue = expectedValue;
    }
  }

  static void testValidCases(TimestampParser tp, ValidTimestampCase[] validCases) {
    for (ValidTimestampCase validCase : validCases) {
      HiveTimestamp ts = tp.parseTimestamp(validCase.strValue);
      assertEquals("Parsing " + validCase.strValue, validCase.expectedValue, ts);
    }
  }

  static void testInvalidCases(TimestampParser tp, String[] invalidCases) {
    for (String invalidString : invalidCases) {
      try {
        HiveTimestamp ts = tp.parseTimestamp(invalidString);
        fail("Expected exception parsing " + invalidString + ", but parsed value to " + ts);
      } catch (IllegalArgumentException err) {
        // Exception expected
      }
    }
  }

  @Test
  public void testDefault() {
    // No timestamp patterns, should default to normal timestamp format
    TimestampParser tp = new TimestampParser();
    ValidTimestampCase[] validCases = {
        new ValidTimestampCase("1945-12-31 23:59:59.0",
            HiveTimestamp.valueOf("1945-12-31 23:59:59.0")),
        new ValidTimestampCase("1945-12-31 23:59:59.1234",
            HiveTimestamp.valueOf("1945-12-31 23:59:59.1234")),
        new ValidTimestampCase("1970-01-01 00:00:00",
            HiveTimestamp.valueOf("1970-01-01 00:00:00")),
    };

    String[] invalidCases = {
        "1945-12-31T23:59:59",
        "12345",
    };

    testValidCases(tp, validCases);
    testInvalidCases(tp, invalidCases);
  }

  @Test
  public void testPattern1() {
    // Joda pattern matching expects fractional seconds length to match
    // the number of 'S' in the pattern. So if you want to match .1, .12, .123,
    // you need 3 different patterns with .S, .SS, .SSS
    String[] patterns = {
        // ISO-8601 timestamps
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-MM-dd'T'HH:mm:ss.S",
        "yyyy-MM-dd'T'HH:mm:ss.SS",
        "yyyy-MM-dd'T'HH:mm:ss.SSS",
        "yyyy-MM-dd'T'HH:mm:ss.SSSS",
    };
    TimestampParser tp = new TimestampParser(patterns);

    ValidTimestampCase[] validCases = {
        new ValidTimestampCase("1945-12-31T23:59:59.0",
            HiveTimestamp.valueOf("1945-12-31 23:59:59.0")),
        new ValidTimestampCase("2001-01-01 00:00:00.100",
            HiveTimestamp.valueOf("2001-01-01 00:00:00.100")),
        new ValidTimestampCase("2001-01-01 00:00:00.001",
            HiveTimestamp.valueOf("2001-01-01 00:00:00.001")),
        // Joda parsing only supports up to millisecond precision
        new ValidTimestampCase("1945-12-31T23:59:59.1234",
            HiveTimestamp.valueOf("1945-12-31 23:59:59.123")),
        new ValidTimestampCase("1970-01-01T00:00:00",
            HiveTimestamp.valueOf("1970-01-01 00:00:00")),
        new ValidTimestampCase("1970-4-5T6:7:8",
            HiveTimestamp.valueOf("1970-04-05 06:07:08")),

        // Default timestamp format still works?
        new ValidTimestampCase("2001-01-01 00:00:00",
            HiveTimestamp.valueOf("2001-01-01 00:00:00")),
        new ValidTimestampCase("1945-12-31 23:59:59.1234",
            HiveTimestamp.valueOf("1945-12-31 23:59:59.1234")),
    };

    String[] invalidCases = {
        "1945-12-31-23:59:59",
        "1945-12-31T23:59:59.12345", // our pattern didn't specify 5 decimal places
        "12345",
    };

    testValidCases(tp, validCases);
    testInvalidCases(tp, invalidCases);
  }

  @Test
  public void testMillisParser() {
    String[] patterns = {
        "millis",
        // Also try other patterns
        "yyyy-MM-dd'T'HH:mm:ss",
    };
    TimestampParser tp = new TimestampParser(patterns);

    ValidTimestampCase[] validCases = {
        new ValidTimestampCase("0", new HiveTimestamp(0)),
        new ValidTimestampCase("-1000000", new HiveTimestamp(-1000000)),
        new ValidTimestampCase("1420509274123", new HiveTimestamp(1420509274123L)),
        new ValidTimestampCase("1420509274123.456789", new HiveTimestamp(1420509274123L)),

        // Other format pattern should also work
        new ValidTimestampCase("1945-12-31T23:59:59",
            HiveTimestamp.valueOf("1945-12-31 23:59:59")),
    };

    String[] invalidCases = {
        "1945-12-31-23:59:59",
        "1945-12-31T23:59:59.12345", // our pattern didn't specify 5 decimal places
        "1420509274123-",
    };

    testValidCases(tp, validCases);
    testInvalidCases(tp, invalidCases);
  }

  @Test
  public void testPattern2() {
    // Pattern does not contain all date fields
    String[] patterns = {
        "HH:mm",
        "MM:dd:ss",
    };
    TimestampParser tp = new TimestampParser(patterns);

    ValidTimestampCase[] validCases = {
        new ValidTimestampCase("05:06",
            HiveTimestamp.valueOf("1970-01-01 05:06:00")),
        new ValidTimestampCase("05:06:07",
            HiveTimestamp.valueOf("1970-05-06 00:00:07")),
    };

    String[] invalidCases = {
        "1945-12-31T23:59:59",
        "1945:12:31-",
        "12345",
    };

    testValidCases(tp, validCases);
    testInvalidCases(tp, invalidCases);
  }
}
