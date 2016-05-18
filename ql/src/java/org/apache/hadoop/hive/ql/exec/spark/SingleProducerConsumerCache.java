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

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;

/**
 * A circular array buffer that supports only one producer and one consumer.
 */
@SuppressWarnings("unchecked")
class SingleProducerConsumerCache {

  @VisibleForTesting
  static final int IN_MEMORY_NUM_ROWS = 2048;

  private final ObjectPair<HiveKey, BytesWritable>[] buffer;

  private volatile int readCursor = 0;
  private volatile int writeCursor = 0;
  private final Object lock = new Object();

  private volatile boolean done;
  private volatile Throwable error;

  public void setDone(Throwable error) {
    synchronized (lock) {
      this.error = error;
      done = true;
      lock.notifyAll();
    }
  }

  public void clear() {
    synchronized (lock) {
      writeCursor = readCursor = 0;
      lock.notifyAll();
    }
  }

  public SingleProducerConsumerCache() {
    buffer = new ObjectPair[IN_MEMORY_NUM_ROWS];
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = new ObjectPair<HiveKey, BytesWritable>();
    }
    done = false;
    error = null;
  }

  private int nextCursor(int cursor) {
    cursor++;
    if (cursor == buffer.length) {
      cursor = 0;
    }
    return cursor;
  }

  private boolean isEmpty() {
    return writeCursor == readCursor;
  }

  private boolean isFull() {
    return nextCursor(writeCursor) == readCursor;
  }

  public void add(HiveKey key, BytesWritable value) {
    if (done) {
      throw new IllegalStateException("Already done and no more data can be written.");
    }
    if (isFull()) {
      synchronized (lock) {
        while (isFull()) {
          if (done) {
            return;
          }
          try {
            lock.wait();
          } catch (InterruptedException e) {
            throw new RuntimeException("Interrupted waiting to write data.", e);
          }
        }
      }
    }
    ObjectPair<HiveKey, BytesWritable> pair = buffer[writeCursor];
    pair.setFirst(key);
    pair.setSecond(value);
    writeCursor = nextCursor(writeCursor);
    // was empty before calling add
    if (writeCursor == nextCursor(readCursor)) {
      synchronized (lock) {
        lock.notify();
      }
    }
  }

  public boolean hasNext() {
    if (isEmpty()) {
      synchronized (lock) {
        while (isEmpty()) {
          if (done) {
            return false;
          }
          try {
            lock.wait();
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
    ObjectPair<HiveKey, BytesWritable> pair = buffer[readCursor];
    Tuple2<HiveKey, BytesWritable> row = new Tuple2<HiveKey, BytesWritable>(
        pair.getFirst(), pair.getSecond());
    pair.setFirst(null);
    pair.setSecond(null);
    readCursor = nextCursor(readCursor);
    // was full before calling next
    if (nextCursor(nextCursor(writeCursor)) == readCursor) {
      synchronized (lock) {
        lock.notify();
      }
    }
    return row;
  }

  public Throwable getError() {
    return error;
  }
}
