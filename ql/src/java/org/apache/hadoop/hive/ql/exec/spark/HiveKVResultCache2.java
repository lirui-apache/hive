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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import scala.Tuple2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.NoSuchElementException;

@SuppressWarnings("unchecked")
public class HiveKVResultCache2 {
  private final ObjectPair<HiveKey, BytesWritable>[] buffer;

  private final Input input;
  private final Output output;

  private File parentFile;
  private volatile File tmpFile;

  private volatile int readCursor = 0;
  private volatile int writeCursor = 0;
  private volatile long numRowsInFile = 0;

  private final Object spillLock;

  private void setupOutput() throws IOException {
    if (parentFile == null) {
      while (true) {
        parentFile = File.createTempFile("hive-resultcache", "");
        if (parentFile.delete() && parentFile.mkdir()) {
          parentFile.deleteOnExit();
          break;
        }
      }
    }

    if (tmpFile == null || input.getInputStream() != null) {
      tmpFile = File.createTempFile("ResultCache", ".tmp", parentFile);
      tmpFile.deleteOnExit();
    }

    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(tmpFile);
      output.setOutputStream(fos);
    } finally {
      if (output.getOutputStream() == null && fos != null) {
        fos.close();
      }
    }
  }

  public HiveKVResultCache2() {
    buffer = new ObjectPair[2048];
    for (int i = 0; i < buffer.length; i++) {
      ObjectPair<HiveKey, BytesWritable> pair = new ObjectPair<>();
      pair.setFirst(new HiveKey());
      pair.setSecond(new BytesWritable());
      buffer[i] = pair;
    }
    input = new Input(4096);
    output = new Output(4096);
    spillLock = new Object();
  }

  private int incrementCursor(int cursor) {
    cursor++;
    if (cursor == buffer.length) {
      cursor = 0;
    }
    return cursor;
  }

  private boolean isFull() {
    return incrementCursor(writeCursor) == readCursor;
  }

  private boolean isEmpty() {
    return writeCursor == readCursor;
  }

  private static void writeHiveKey(Output output, HiveKey hiveKey) {
    int size = hiveKey.getLength();
    output.writeInt(size);
    output.writeBytes(hiveKey.getBytes(), 0, size);
    output.writeInt(hiveKey.hashCode());
    output.writeInt(hiveKey.getDistKeyLength());
  }

  private static void readHiveKey(Input input, HiveKey hiveKey) {
    byte[] bytes = input.readBytes(input.readInt());
    int hashCode = input.readInt();
    int distKeyLen = input.readInt();
    hiveKey.setHashCode(hashCode);
    hiveKey.setDistKeyLength(distKeyLen);
    hiveKey.set(bytes, 0, bytes.length);
  }

  private static void readValue(Input input, BytesWritable value) {
    byte[] bytes = input.readBytes(input.readInt());
    value.set(bytes, 0, bytes.length);
  }

  private static void writeValue(Output output, BytesWritable bytesWritable) {
    int size = bytesWritable.getLength();
    output.writeInt(size);
    output.writeBytes(bytesWritable.getBytes(), 0, size);
  }

  public void add(HiveKey key, BytesWritable value) {
    if (isFull()) {
      // spill
      synchronized (spillLock) {
        try {
          if (output.getOutputStream() == null) {
            setupOutput();
          }
          writeHiveKey(output, key);
          writeValue(output, value);
          numRowsInFile++;
        } catch (IOException e) {
          throw new RuntimeException("Failed to spill rows to disk", e);
        }
      }
    } else {
      ObjectPair<HiveKey, BytesWritable> pair = buffer[writeCursor];
      HiveKey reusedKey = pair.getFirst();
      reusedKey.setHashCode(key.hashCode());
      reusedKey.setDistKeyLength(key.getDistKeyLength());
      reusedKey.set(key);
      BytesWritable reusedValue = pair.getSecond();
      reusedValue.set(value);
      writeCursor = incrementCursor(writeCursor);
    }
  }

  public boolean hasNext() {
    return !isEmpty() || numRowsInFile > 0;
  }

  public Tuple2<HiveKey, BytesWritable> next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more data to read.");
    }
    if (isEmpty()) {
      // read from file
      synchronized (spillLock) {
        try {
          Preconditions.checkState(input.getInputStream() != null || output.getOutputStream() != null);
          if (input.getInputStream() == null) {
            output.close();
            output.setOutputStream(null);

            FileInputStream fis = null;
            try {
              fis = new FileInputStream(tmpFile);
              input.setInputStream(fis);
            } finally {
              if (input.getInputStream() == null && fis != null) {
                fis.close();
              }
            }
          }
          HiveKey key = new HiveKey();
          BytesWritable value = new BytesWritable();
          readHiveKey(input, key);
          readValue(input, value);
          numRowsInFile--;
          if (input.eof()) {
            input.close();
            input.setInputStream(null);
          }
          return new Tuple2<>(key, value);
        } catch (IOException e) {
          throw new RuntimeException("Failed to load rows from disk", e);
        }
      }
    } else {
      ObjectPair<HiveKey, BytesWritable> pair = buffer[readCursor];
      readCursor = incrementCursor(readCursor);
      return new Tuple2<>(SparkUtilities.copyHiveKey(pair.getFirst()),
          SparkUtilities.copyBytesWritable(pair.getSecond()));
    }
  }

  public void clear() {
    writeCursor = readCursor = 0;

    if (parentFile != null) {
      try {
        FileUtil.fullyDelete(parentFile);
      } catch (Throwable ignored) {
      }
      parentFile = null;
      tmpFile = null;
    }

    try {
      input.close();
      output.close();
      input.setInputStream(null);
      output.setOutputStream(null);
    } catch (Throwable ignored) {
    }
  }
}
