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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.TimeZone;
import java.util.concurrent.ThreadLocalRandom;

public class TestHiveTimestamp {

  private static TimeZone defaultTZ;
  private static final String[] IDs = TimeZone.getAvailableIDs();

  @BeforeClass
  public static void storeDefaultTZ() {
    defaultTZ = TimeZone.getDefault();
  }

  @Before
  public void setTZ() {
    int index = ThreadLocalRandom.current().nextInt(IDs.length);
    TimeZone.setDefault(TimeZone.getTimeZone(IDs[index]));
  }

  @AfterClass
  public static void restoreTZ() {
    TimeZone.setDefault(defaultTZ);
  }

  @Test
  public void testParse() {
    // No timezone specified
    String s1 = "2016-01-03 12:26:34.0123";
    Assert.assertEquals(s1, HiveTimestamp.valueOf(s1).toString());
    // With timezone
    String s2 = s1 + " UTC";
    Assert.assertEquals(s1 + " GMT", HiveTimestamp.valueOf(s2).toString());
    Assert.assertEquals(s1 + " GMT+08:00", HiveTimestamp.valueOf(s1, "Asia/Shanghai").toString());
  }

  @Test
  public void testHandleDST() {
    // Same timezone can have different offset due to DST
    String s1 = "2005-01-03 02:01:00";
    Assert.assertEquals(s1 + ".0 GMT", HiveTimestamp.valueOf(s1, "Europe/London").toString());
    String s2 = "2005-06-03 02:01:00.30547";
    Assert.assertEquals(s2 + " GMT+01:00", HiveTimestamp.valueOf(s2, "Europe/London").toString());
    // Can print time with DST properly
    String s3 = "2005-04-03 02:01:00.04067";
    Assert.assertEquals("2005-04-03 03:01:00.04067 GMT-07:00",
        HiveTimestamp.valueOf(s3, "America/Los_Angeles").toString());
  }

  @Test
  public void testBadZoneID() {
    try {
      new HiveTimestamp(0, "Foo id");
      Assert.fail("Invalid timezone ID should cause exception");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
