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
package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import scala.Tuple2;

import com.google.common.base.Preconditions;

/**
 * Base class for
 *   - collecting Map/Reduce function output and
 *   - providing an Iterable interface for fetching output records. Input records
 *     are processed in lazy fashion i.e when output records are requested
 *     through Iterator interface.
 */
@SuppressWarnings("rawtypes")
public abstract class HiveBaseFunctionResultList<T> implements
    Iterable, OutputCollector<HiveKey, BytesWritable>, Serializable {
  private static final long serialVersionUID = -1L;
  private final Iterator<T> inputIterator;
  private boolean isClosed = false;
  private final boolean newResultCache;

  // Contains results from last processed input record.
  private final HiveKVResultCache lastRecordOutput;
  private SingleFileBasedResultCache resultCache;
  private boolean iteratorAlreadyCreated = false;

  public HiveBaseFunctionResultList(Iterator<T> inputIterator, Configuration conf) {
    this.inputIterator = inputIterator;
    this.lastRecordOutput = new HiveKVResultCache();
    newResultCache = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SPARK_NEW_RESULTCACHE);
  }

  @Override
  public Iterator iterator() {
    Preconditions.checkState(!iteratorAlreadyCreated, "Iterator can only be created once.");
    iteratorAlreadyCreated = true;
    return newResultCache ? new NewResultIterator() : new ResultIterator();
  }

  @Override
  public void collect(HiveKey key, BytesWritable value) throws IOException {
    if (newResultCache) {
      resultCache.add(SparkUtilities.copyHiveKey(key),
          SparkUtilities.copyBytesWritable(value));
    } else {
      lastRecordOutput.add(SparkUtilities.copyHiveKey(key),
          SparkUtilities.copyBytesWritable(value));
    }
  }

  /** Process the given record. */
  protected abstract void processNextRecord(T inputRecord) throws IOException;

  /**
   * @return true if current state of the record processor is done.
   */
  protected abstract boolean processingDone();

  /** Close the record processor. */
  protected abstract void closeRecordProcessor();

  public class NewResultIterator implements Iterator {

    private class ProcessorRunnable implements Runnable {

      @Override
      public void run() {
        while (inputIterator.hasNext() && !processingDone()) {
          try {
            processNextRecord(inputIterator.next());
          } catch (IOException e) {
            resultCache.setDone(e);
            throw new IllegalStateException("Error while processing input.", e);
          }
        }
        if (!isClosed) {
          closeRecordProcessor();
          isClosed = true;
        }
        // make sure to mark the cache done after closing the record processor
        resultCache.setDone(null);
      }
    }

    public NewResultIterator() {
      try {
        resultCache = new SingleFileBasedResultCache();
      } catch (IOException e) {
        throw new IllegalStateException("Failed to create result cache.", e);
      }
      Thread processor = new Thread(new ProcessorRunnable());
      processor.setDaemon(true);
      processor.setName("RecordProcessorThread");
      processor.start();
    }

    @Override
    public boolean hasNext() {
      if (resultCache.hasNext()) {
        return true;
      }
      resultCache.clear();
      return false;
    }

    @Override
    public Tuple2<HiveKey, BytesWritable> next() {
      if (hasNext()) {
        return resultCache.next();
      }
      throw new NoSuchElementException("There are no more elements");
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Iterator.remove() is not supported");
    }
  }

  /** Implement Iterator interface. */
  public class ResultIterator implements Iterator {
    @Override
    public boolean hasNext(){
      // Return remaining records (if any) from last processed input record.
      if (lastRecordOutput.hasNext()) {
        return true;
      }

      // Process the records in the input iterator until
      //  - new output records are available for serving downstream operator,
      //  - input records are exhausted or
      //  - processing is completed.
      while (inputIterator.hasNext() && !processingDone()) {
        try {
          processNextRecord(inputIterator.next());
          if (lastRecordOutput.hasNext()) {
            return true;
          }
        } catch (IOException ex) {
          throw new IllegalStateException("Error while processing input.", ex);
        }
      }

      // At this point we are done processing the input. Close the record processor
      if (!isClosed) {
        closeRecordProcessor();
        isClosed = true;
      }

      // It is possible that some operators add records after closing the processor, so make sure
      // to check the lastRecordOutput
      if (lastRecordOutput.hasNext()) {
        return true;
      }

      lastRecordOutput.clear();
      return false;
    }

    @Override
    public Tuple2<HiveKey, BytesWritable> next() {
      if (hasNext()) {
        return lastRecordOutput.next();
      }
      throw new NoSuchElementException("There are no more elements");
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Iterator.remove() is not supported");
    }
  }
}
