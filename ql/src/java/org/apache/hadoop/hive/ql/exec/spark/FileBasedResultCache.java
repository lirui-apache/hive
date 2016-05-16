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
package org.apache.hadoop.hive.ql.exec.spark;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;

import scala.Tuple2;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * A cache with fixed buffer. If the buffer is full, new entries will
 * be written to disk. This class is thread safe since multiple threads
 * could access it (doesn't have to be concurrently), for example,
 * the StreamThread in ScriptOperator.
 */
@SuppressWarnings("unchecked")
class FileBasedResultCache {
  private static final Logger LOG = LoggerFactory.getLogger(FileBasedResultCache.class);

  @VisibleForTesting
  static final int IN_MEMORY_NUM_ROWS = 1024;

  private final ObjectPair<HiveKey, BytesWritable>[] buffer;

  private volatile int readCursor = 0;
  private volatile int writeCursor = 0;
  private volatile int size = 0;

  private volatile boolean done;
  private volatile Throwable error;

  public synchronized void setDone(Throwable error) {
    this.error = error;
    done = true;
    notifyAll();
  }

  public FileBasedResultCache() {
    buffer = new ObjectPair[IN_MEMORY_NUM_ROWS * 2];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = new ObjectPair<HiveKey, BytesWritable>();
    }
    done = false;
    error = null;
  }

  public synchronized void add(HiveKey key, BytesWritable value) {
    if (done) {
      throw new IllegalStateException("Already done and no more data can be written.");
    }
    while (size >= buffer.length) {
      if (done) {
        throw new IllegalStateException("Already done and no more data can be written.");
      }
      try {
        wait();
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted waiting to write data.", e);
      }
    }
    ObjectPair<HiveKey, BytesWritable> pair = buffer[writeCursor++];
    pair.setFirst(key);
    pair.setSecond(value);
    if (writeCursor == buffer.length) {
      writeCursor = 0;
    }
    size++;
    notifyAll();
  }

  public synchronized void clear() {
    writeCursor = readCursor  = 0;
    notifyAll();
  }

  public synchronized boolean hasNext() {
    while (size <= 0) {
      if (done) {
        return false;
      }
      try {
        wait();
      } catch (InterruptedException e) {
        setDone(e);
        throw new RuntimeException("Interrupted while waiting for data.", e);
      }
    }
    return error == null;
  }

  public synchronized Tuple2<HiveKey, BytesWritable> next() {
    Preconditions.checkState(hasNext());
    ObjectPair<HiveKey, BytesWritable> pair = buffer[readCursor++];
    Tuple2<HiveKey, BytesWritable> row = new Tuple2<HiveKey, BytesWritable>(
        pair.getFirst(), pair.getSecond());
    pair.setFirst(null);
    pair.setSecond(null);
    if (readCursor == buffer.length) {
      readCursor = 0;
    }
    size--;
    notifyAll();
    return row;
  }

  public Throwable getError() {
    return error;
  }
}
