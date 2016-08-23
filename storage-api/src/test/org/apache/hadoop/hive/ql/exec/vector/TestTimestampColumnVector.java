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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.common.type.HiveTimestamp;
import org.junit.Test;

import java.util.Random;

import org.apache.hadoop.hive.common.type.RandomTypeUtil;

import static org.junit.Assert.*;

/**
 * Test for ListColumnVector
 */
public class TestTimestampColumnVector {

  private static int TEST_COUNT = 5000;

  private static int fake = 0;

  @Test
  public void testSaveAndRetrieve() throws Exception {

    Random r = new Random(1234);
    TimestampColumnVector timestampColVector = new TimestampColumnVector();
    HiveTimestamp[] randTimestamps = new HiveTimestamp[VectorizedRowBatch.DEFAULT_SIZE];

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      HiveTimestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r);
      randTimestamps[i] = randTimestamp;
      timestampColVector.set(i, randTimestamp);
    }
    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      HiveTimestamp retrievedTimestamp = timestampColVector.asScratchTimestamp(i);
      HiveTimestamp randTimestamp = randTimestamps[i];
      if (!retrievedTimestamp.equals(randTimestamp)) {
        assertTrue(false);
      }
    }
  }

  @Test
  public void testTimestampCompare() throws Exception {
    Random r = new Random(1234);
    TimestampColumnVector timestampColVector = new TimestampColumnVector();
    HiveTimestamp[] randTimestamps = new HiveTimestamp[VectorizedRowBatch.DEFAULT_SIZE];
    HiveTimestamp[] candTimestamps = new HiveTimestamp[VectorizedRowBatch.DEFAULT_SIZE];
    int[] compareToLeftRights = new int[VectorizedRowBatch.DEFAULT_SIZE];
    int[] compareToRightLefts = new int[VectorizedRowBatch.DEFAULT_SIZE];

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      HiveTimestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r);
      randTimestamps[i] = randTimestamp;
      timestampColVector.set(i, randTimestamp);
      HiveTimestamp candTimestamp = RandomTypeUtil.getRandTimestamp(r);
      candTimestamps[i] = candTimestamp;
      compareToLeftRights[i] = candTimestamp.compareTo(randTimestamp);
      compareToRightLefts[i] = randTimestamp.compareTo(candTimestamp);
    }

    for (int i = 0; i < VectorizedRowBatch.DEFAULT_SIZE; i++) {
      HiveTimestamp retrievedTimestamp = timestampColVector.asScratchTimestamp(i);
      HiveTimestamp randTimestamp = randTimestamps[i];
      if (!retrievedTimestamp.equals(randTimestamp)) {
        assertTrue(false);
      }
      HiveTimestamp candTimestamp = candTimestamps[i];
      int compareToLeftRight = timestampColVector.compareTo(candTimestamp, i);
      if (compareToLeftRight != compareToLeftRights[i]) {
        assertTrue(false);
      }
      int compareToRightLeft = timestampColVector.compareTo(i, candTimestamp);
      if (compareToRightLeft != compareToRightLefts[i]) {
        assertTrue(false);
      }
    }
  }

  /*
  @Test
  public void testGenerate() throws Exception {
    PrintWriter writer = new PrintWriter("/Users/you/timestamps.txt");
    Random r = new Random(18485);
    for (int i = 0; i < 25; i++) {
      Timestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r);
      writer.println(randTimestamp.toString());
    }
    for (int i = 0; i < 25; i++) {
      Timestamp randTimestamp = RandomTypeUtil.getRandTimestamp(r, 1965, 2025);
      writer.println(randTimestamp.toString());
    }
    writer.close();
  }
  */
}
