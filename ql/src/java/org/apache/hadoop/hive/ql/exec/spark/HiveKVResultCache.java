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
import java.util.Arrays;
import java.util.NoSuchElementException;

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
class HiveKVResultCache {
  private static final Logger LOG = LoggerFactory.getLogger(HiveKVResultCache.class);

  @VisibleForTesting
  static final int IN_MEMORY_NUM_ROWS = 1024;

  private ObjectPair<HiveKey, BytesWritable>[] writeBuffer;
  private ObjectPair<HiveKey, BytesWritable>[] readBuffer;

  private File parentFile;
  private File tmpFile;

  private int readCursor = 0;
  private int writeCursor = 0;

  // Indicate if the read buffer has data, for example,
  // when in reading, data on disk could be pull in
  private boolean readBufferUsed = false;
  private int rowsInReadBuffer = 0;

  private Input input;
  private Output output;

  public HiveKVResultCache() {
    writeBuffer = new ObjectPair[IN_MEMORY_NUM_ROWS];
    readBuffer = new ObjectPair[IN_MEMORY_NUM_ROWS];
    for (int i = 0; i < IN_MEMORY_NUM_ROWS; i++) {
      writeBuffer[i] = new ObjectPair<HiveKey, BytesWritable>();
      writeBuffer[i].setFirst(new HiveKey());
      writeBuffer[i].setSecond(new BytesWritable());
      readBuffer[i] = new ObjectPair<HiveKey, BytesWritable>();
      readBuffer[i].setFirst(new HiveKey());
      readBuffer[i].setSecond(new BytesWritable());
    }
    input = new Input(4096);
    output = new Output(4096);
  }

  private void switchBufferAndResetCursor() {
    ObjectPair<HiveKey, BytesWritable>[] tmp = readBuffer;
    rowsInReadBuffer = writeCursor;
    readBuffer = writeBuffer;
    readBufferUsed = true;
    readCursor = 0;
    writeBuffer = tmp;
    writeCursor = 0;
  }

  private void setupOutput() throws IOException {
    if (parentFile == null) {
      while (true) {
        parentFile = File.createTempFile("hive-resultcache", "");
        if (parentFile.delete() && parentFile.mkdir()) {
          parentFile.deleteOnExit();
          break;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Retry creating tmp result-cache directory...");
        }
      }
    }

    if (tmpFile == null || input.getInputStream() != null) {
      tmpFile = File.createTempFile("ResultCache", ".tmp", parentFile);
      LOG.info("ResultCache created temp file " + tmpFile.getAbsolutePath());
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

  private void readValue(Input input, BytesWritable value) {
    byte[] bytes = input.readBytes(input.readInt());
    value.set(bytes, 0, bytes.length);
  }

  private void writeValue(Output output, BytesWritable bytesWritable) {
    int size = bytesWritable.getLength();
    output.writeInt(size);
    output.writeBytes(bytesWritable.getBytes(), 0, size);
  }

  private void readHiveKey(Input input, HiveKey hiveKey) {
    byte[] bytes = input.readBytes(input.readInt());
    int hashCode = input.readInt();
    int distKeyLen = input.readInt();
    hiveKey.setHashCode(hashCode);
    hiveKey.setDistKeyLength(distKeyLen);
    hiveKey.set(bytes, 0, bytes.length);
  }

  private void writeHiveKey(Output output, HiveKey hiveKey) {
    int size = hiveKey.getLength();
    output.writeInt(size);
    output.writeBytes(hiveKey.getBytes(), 0, size);
    output.writeInt(hiveKey.hashCode());
    output.writeInt(hiveKey.getDistKeyLength());
  }

  public synchronized void add(HiveKey key, BytesWritable value) {
    if (writeCursor >= IN_MEMORY_NUM_ROWS) { // Write buffer is full
      if (!readBufferUsed) { // Read buffer isn't used, switch buffer
        switchBufferAndResetCursor();
      } else {
        // Need to spill from write buffer to disk
        try {
          if (output.getOutputStream() == null) {
            setupOutput();
          }
          for (int i = 0; i < IN_MEMORY_NUM_ROWS; i++) {
            ObjectPair<HiveKey, BytesWritable> pair = writeBuffer[i];
            writeHiveKey(output, pair.getFirst());
            writeValue(output, pair.getSecond());
          }
          writeCursor = 0;
        } catch (Exception e) {
          clear(); // Clean up the cache
          throw new RuntimeException("Failed to spill rows to disk", e);
        }
      }
    }
    ObjectPair<HiveKey, BytesWritable> pair = writeBuffer[writeCursor++];
    HiveKey reusedKey = pair.getFirst();
    reusedKey.setHashCode(key.hashCode());
    reusedKey.setDistKeyLength(key.getDistKeyLength());
    reusedKey.set(key);
    BytesWritable reusedValue = pair.getSecond();
    reusedValue.set(value);
  }

  public synchronized void clear() {
    writeCursor = readCursor = rowsInReadBuffer = 0;
    readBufferUsed = false;

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

  public synchronized boolean hasNext() {
    return readBufferUsed || writeCursor > 0;
  }

  public synchronized Tuple2<HiveKey, BytesWritable> next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more data to read.");
    }
    if (!readBufferUsed) {
      try {
        if (input.getInputStream() == null && output.getOutputStream() != null) {
          // Close output stream if open
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
        if (input.getInputStream() != null) {
          // Load next batch from disk
          for (int i = 0; i < IN_MEMORY_NUM_ROWS; i++) {
            ObjectPair<HiveKey, BytesWritable> pair = readBuffer[i];
            readHiveKey(input, pair.getFirst());
            readValue(input, pair.getSecond());
          }
          if (input.eof()) {
            input.close();
            input.setInputStream(null);
          }
          rowsInReadBuffer = IN_MEMORY_NUM_ROWS;
          readBufferUsed = true;
          readCursor = 0;
        } else if (writeCursor == 1) {
          ObjectPair<HiveKey, BytesWritable> pair = writeBuffer[0];
          Tuple2<HiveKey, BytesWritable> row = new Tuple2<HiveKey, BytesWritable>(
              SparkUtilities.copyHiveKey(pair.getFirst()),
              SparkUtilities.copyBytesWritable(pair.getSecond()));
          writeCursor = 0;
          return row;
        } else {
          // No record on disk, more data in write buffer
          switchBufferAndResetCursor();
        }
      } catch (Exception e) {
        clear(); // Clean up the cache
        throw new RuntimeException("Failed to load rows from disk", e);
      }
    }
    ObjectPair<HiveKey, BytesWritable> pair = readBuffer[readCursor];
    Tuple2<HiveKey, BytesWritable> row = new Tuple2<HiveKey, BytesWritable>(
        SparkUtilities.copyHiveKey(pair.getFirst()),
        SparkUtilities.copyBytesWritable(pair.getSecond()));
    if (++readCursor >= rowsInReadBuffer) {
      readBufferUsed = false;
      rowsInReadBuffer = 0;
      readCursor = 0;
    }
    return row;
  }
}
