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

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class AggressiveResultCache {
  private static final int numRecordsInMem = 2048;

  private final Queue<ObjectPair<HiveKey, BytesWritable>> buffer;
  private volatile boolean done;
  private final Object lock;
  private volatile Throwable error;

  public AggressiveResultCache() {
    buffer = new LinkedBlockingQueue<>(numRecordsInMem);
    done = false;
    lock = new Object();
  }

  public void add(HiveKey key, BytesWritable value) {
    if (done) {
      throw new IllegalStateException("Already done and no more data can be written.");
    }
    buffer.add(new ObjectPair<>(key, value));
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  public void setDone(Throwable error) {
    done = true;
    this.error = error;
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  public boolean hasNext() {
    while (buffer.isEmpty()) {
      if (done) {
        return false;
      }
      synchronized (lock) {
        try {
          lock.wait(2000);
        } catch (InterruptedException e) {
          setDone(e);
          throw new RuntimeException("Interrupted while waiting.", e);
        }
      }
    }
    return error == null;
  }

  // Caller is responsible to check hasNext first.
  public Tuple2<HiveKey, BytesWritable> next() {
    ObjectPair<HiveKey, BytesWritable> pair = buffer.remove();
    return new Tuple2<>(pair.getFirst(), pair.getSecond());
  }

  public void clear() {
    buffer.clear();
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  public Throwable getError() {
    return error;
  }
}
