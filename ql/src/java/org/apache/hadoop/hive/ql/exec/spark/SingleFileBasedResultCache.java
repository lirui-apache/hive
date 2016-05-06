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

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * A producer & consumer solution that doesn't block the producer. If the buffer
 * in memory is full, data can be spilled to disk.
 * The order that the data is read is NOT guaranteed to be the same as they are written.
 */
public class SingleFileBasedResultCache {
  private static final Logger LOG = LoggerFactory.getLogger(SingleFileBasedResultCache.class);
  private static final int numRecordsInMem = 2048;

  private final Queue<ObjectPair<HiveKey, BytesWritable>> buffer;
  private final File file;
  private final RandomAccessFile spillFile;
  private volatile long readPosInFile;
  private volatile long writePosInFile;
  private volatile long recordsInFile;
  private final Object lock = new Object();
  private volatile boolean done;
  private volatile Throwable error;

  public SingleFileBasedResultCache() throws IOException {
    buffer = new ArrayDeque<>(numRecordsInMem);
    File parentDir;
    while (true) {
      parentDir = File.createTempFile("hive-resultcache", "");
      if (parentDir.delete() && parentDir.mkdir()) {
        parentDir.deleteOnExit();
        break;
      }
    }
    file = File.createTempFile("ResultCache", ".tmp", parentDir);
    spillFile = new RandomAccessFile(file, "rws");
    readPosInFile = 0;
    writePosInFile = 0;
    recordsInFile = 0;
    error = null;
  }

  // Indicate that no more data will be written.
  public void setDone(Throwable error) {
    synchronized (lock) {
      done = true;
      this.error = error;
      lock.notifyAll();
    }
  }

  public void clear() {
    synchronized (lock) {
      buffer.clear();
      try {
        spillFile.close();
      } catch (IOException e) {
        // ignore
      }
      file.delete();
      recordsInFile = 0;
      readPosInFile = 0;
      writePosInFile = 0;
      lock.notifyAll();
    }
  }

  private void writeHiveKey(HiveKey hiveKey) throws IOException {
    int size = hiveKey.getLength();
    spillFile.writeInt(size);
    spillFile.write(hiveKey.getBytes(), 0, size);
    spillFile.writeInt(hiveKey.hashCode());
    spillFile.writeInt(hiveKey.getDistKeyLength());
  }

  private void writeValue(BytesWritable bytesWritable) throws IOException {
    int size = bytesWritable.getLength();
    spillFile.writeInt(size);
    spillFile.write(bytesWritable.getBytes(), 0, size);
  }

  private HiveKey readHiveKey() throws IOException {
    int size = spillFile.readInt();
    byte[] bytes = new byte[size];
    spillFile.read(bytes, 0, size);
    HiveKey hiveKey = new HiveKey(bytes, spillFile.readInt());
    hiveKey.setDistKeyLength(spillFile.readInt());
    return hiveKey;
  }

  private BytesWritable readValue() throws IOException {
    byte[] bytes = new byte[spillFile.readInt()];
    spillFile.read(bytes, 0, bytes.length);
    return new BytesWritable(bytes);
  }

  // Spill half of the buffer to disk. Returns the number of records spilled.
  private int spill() throws IOException {
    Preconditions.checkState(!buffer.isEmpty());
    int toSpill = Math.max(1, buffer.size() / 2);
    spillFile.seek(writePosInFile);
    for (int i = 0; i < toSpill; i++) {
      ObjectPair<HiveKey, BytesWritable> pair = buffer.remove();
      writeHiveKey(pair.getFirst());
      writeValue(pair.getSecond());
    }
    recordsInFile += toSpill;
    writePosInFile = spillFile.getFilePointer();
    return toSpill;
  }

  public boolean hasNext() {
    synchronized (lock) {
      if (error != null) {
        throw new IllegalStateException("Error inside the result cache.", error);
      }
      while (buffer.isEmpty() && recordsInFile <= 0) {
        if (done) {
          return false;
        }
        try {
          lock.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException("Interrupted while waiting.", e);
        }
      }
      return true;
    }
  }

  private int load() throws IOException {
    Preconditions.checkArgument(recordsInFile > 0 && buffer.isEmpty());
    int toLoad = numRecordsInMem / 2 > recordsInFile ? (int) recordsInFile : numRecordsInMem / 2;
    toLoad = Math.max(1, toLoad);
    spillFile.seek(readPosInFile);
    for (int i = 0; i < toLoad; i++) {
      buffer.add(new ObjectPair<>(readHiveKey(), readValue()));
    }
    recordsInFile -= toLoad;
    readPosInFile = spillFile.getFilePointer();
    return toLoad;
  }

  public Tuple2<HiveKey, BytesWritable> next() {
    synchronized (lock) {
      if (!hasNext()) {
        throw new NoSuchElementException("No more data to read.");
      }
      if (buffer.isEmpty()) {
        try {
          load();
        } catch (IOException e) {
          // inform writer to stop writing
          setDone(e);
          throw new RuntimeException("Failed to load rows from disk.", e);
        }
      }
      ObjectPair<HiveKey, BytesWritable> pair = buffer.remove();
      return new Tuple2<>(pair.getFirst(), pair.getSecond());
    }
  }

  public void add(HiveKey key, BytesWritable value) {
    synchronized (lock) {
      if (done) {
        throw new IllegalStateException("Already done and no more data can be written.");
      }
      if (buffer.size() >= numRecordsInMem) {
        try {
          spill();
        } catch (IOException e) {
          setDone(e);
          throw new RuntimeException("Failed to spill rows to disk.", e);
        }
      }
      buffer.add(new ObjectPair<>(key, value));
      lock.notifyAll();
    }
  }
}
