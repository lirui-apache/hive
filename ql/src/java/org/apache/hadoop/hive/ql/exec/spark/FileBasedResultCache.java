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
import java.util.concurrent.atomic.AtomicInteger;

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

  @VisibleForTesting
  static final int IN_MEMORY_NUM_ROWS = 2048;

  private final ObjectPair<HiveKey, BytesWritable>[] buffer;

  private volatile int readCursor = 0;
  private volatile int writeCursor = 0;
  private final AtomicInteger size = new AtomicInteger(0);

  private volatile boolean done;
  private volatile Throwable error;

  public void setDone(Throwable error) {
    synchronized (size) {
      this.error = error;
      done = true;
      size.notifyAll();
    }
  }

  public void clear() {
    synchronized (size) {
      writeCursor = readCursor = 0;
      size.notifyAll();
    }
  }

  public FileBasedResultCache() {
    buffer = new ObjectPair[IN_MEMORY_NUM_ROWS];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = new ObjectPair<HiveKey, BytesWritable>();
    }
    done = false;
    error = null;
  }

  public void add(HiveKey key, BytesWritable value) {
    if (done) {
      throw new IllegalStateException("Already done and no more data can be written.");
    }
    if (size.get() >= buffer.length) {
      synchronized (size) {
        while (size.get() >= buffer.length) {
          if (done) {
            throw new IllegalStateException("Already done and no more data can be written.");
          }
          try {
            size.wait();
          } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted waiting to write data.", e);
          }
        }
      }
    }
    doAdd(key, value);
  }

  private void doAdd(HiveKey key, BytesWritable value) {
    ObjectPair<HiveKey, BytesWritable> pair = buffer[writeCursor++];
    pair.setFirst(key);
    pair.setSecond(value);
    if (writeCursor == buffer.length) {
      writeCursor = 0;
    }
    if (size.getAndIncrement() == 0) {
      synchronized (size) {
        size.notifyAll();
      }
    }
  }

  public boolean hasNext() {
    if (size.get() <= 0 && !done) {
      synchronized (size) {
        while (size.get() <= 0) {
          if (done) {
            return false;
          }
          try {
            size.wait();
          } catch (InterruptedException e) {
            setDone(e);
            throw new RuntimeException("Interrupted while waiting for data.", e);
          }
        }
      }
    }
    return error == null;
  }

  public Tuple2<HiveKey, BytesWritable> next() {
    ObjectPair<HiveKey, BytesWritable> pair = buffer[readCursor++];
    Tuple2<HiveKey, BytesWritable> row = new Tuple2<HiveKey, BytesWritable>(
        pair.getFirst(), pair.getSecond());
    pair.setFirst(null);
    pair.setSecond(null);
    if (readCursor == buffer.length) {
      readCursor = 0;
    }
    if (size.getAndDecrement() == buffer.length) {
      synchronized (size) {
        size.notifyAll();
      }
    }
    return row;
  }

  public Throwable getError() {
    return error;
  }
}
