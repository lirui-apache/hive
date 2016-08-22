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
package org.apache.hadoop.hive.serde2.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveTimestamp;
import org.apache.hadoop.hive.ql.util.TimestampUtils;
import org.apache.hadoop.hive.serde2.ByteStream.RandomAccessOutput;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryUtils.VInt;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

/**
 * TimestampWritable
 * Writable equivalent of java.sq.Timestamp
 *
 * Timestamps are of the format
 *    YYYY-MM-DD HH:MM:SS.[fff...]
 *
 * We encode Unix timestamp in seconds in 4 bytes, using the MSB to signify
 * whether the timestamp has a fractional portion.
 *
 * The fractional portion is reversed, and encoded as a VInt
 * so timestamps with less precision use fewer bytes.
 *
 *      0.1    -> 1
 *      0.01   -> 10
 *      0.001  -> 100
 *
 */
public class TimestampWritable implements WritableComparable<TimestampWritable> {

  static final public byte[] nullBytes = {0x0, 0x0, 0x0, 0x0};

  private static final int DECIMAL_OR_SECOND_VINT_FLAG = 0x80000000;
  private static final int LOWEST_31_BITS_OF_SEC_MASK = 0x7fffffff;

  private static final long SEVEN_BYTE_LONG_SIGN_FLIP = 0xff80L << 48;

  private static final int TIMEZONE_MASK = 1 << 30;


  /** The maximum number of bytes required for a TimestampWritable */
  public static final int MAX_BYTES = 16;

  public static final int BINARY_SORTABLE_LENGTH = 15;

  private HiveTimestamp timestamp = new HiveTimestamp(0);

  /**
   * true if data is stored in timestamp field rather than byte arrays.
   *      allows for lazy conversion to bytes when necessary
   * false otherwise
   */
  private boolean bytesEmpty;
  private boolean timestampEmpty;

  /* Allow use of external byte[] for efficiency */
  private byte[] currentBytes;
  private final byte[] internalBytes = new byte[MAX_BYTES];
  private byte[] externalBytes;
  private int offset;

  /* Constructors */
  public TimestampWritable() {
    bytesEmpty = false;
    currentBytes = internalBytes;
    offset = 0;

    clearTimestamp();
  }

  public TimestampWritable(byte[] bytes, int offset) {
    set(bytes, offset);
  }

  public TimestampWritable(TimestampWritable t) {
    this(t.getBytes(), 0);
  }

  public TimestampWritable(HiveTimestamp t) {
    set(t);
  }

  public void set(byte[] bytes, int offset) {
    externalBytes = bytes;
    this.offset = offset;
    bytesEmpty = false;
    currentBytes = externalBytes;

    clearTimestamp();
  }

  public void setTime(long time) {
    timestamp.setTime(time);
    bytesEmpty = true;
    timestampEmpty = false;
  }

  public void set(HiveTimestamp t) {
    if (t == null) {
      timestamp.setTime(0);
      timestamp.setNanos(0);
      timestamp.setOffsetInMin(null);
      return;
    }
    this.timestamp = t;
    bytesEmpty = true;
    timestampEmpty = false;
  }

  public void set(TimestampWritable t) {
    if (t.bytesEmpty) {
      set(t.getTimestamp());
      return;
    }
    if (t.currentBytes == t.externalBytes) {
      set(t.currentBytes, t.offset);
    } else {
      set(t.currentBytes, 0);
    }
  }

  private static void updateTimestamp(HiveTimestamp timestamp, long secondsAsMillis, int nanos) {
    timestamp.setTime(secondsAsMillis);
    timestamp.setNanos(nanos);
  }

  public void setInternal(long secondsAsMillis, int nanos) {

    // This is our way of documenting that we are MUTATING the contents of
    // this writable's internal timestamp.
    updateTimestamp(timestamp, secondsAsMillis, nanos);

    bytesEmpty = true;
    timestampEmpty = false;
  }

  private void clearTimestamp() {
    timestampEmpty = true;
  }

  public void writeToByteStream(RandomAccessOutput byteStream) {
    checkBytes();
    byteStream.write(currentBytes, offset, getTotalLength());
  }

  /**
   *
   * @return seconds corresponding to this TimestampWritable
   */
  public long getSeconds() {
    if (!timestampEmpty) {
      return TimestampUtils.millisToSeconds(timestamp.getTime());
    } else if (!bytesEmpty) {
      return TimestampWritable.getSeconds(currentBytes, offset);
    } else {
      throw new IllegalStateException("Both timestamp and bytes are empty");
    }
  }

  /**
   *
   * @return nanoseconds in this TimestampWritable
   */
  public int getNanos() {
    if (!timestampEmpty) {
      return timestamp.getNanos();
    } else if (!bytesEmpty) {
      return hasDecimalOrSecondVInt() ?
          TimestampWritable.getNanos(currentBytes, offset + 4) : 0;
    } else {
      throw new IllegalStateException("Both timestamp and bytes are empty");
    }
  }

  private Integer getTimezoneOffset() {
    if (!timestampEmpty) {
      return timestamp.getOffsetInMin();
    } else if (!bytesEmpty) {
      return hasDecimalOrSecondVInt() ? getTimezoneOffset(currentBytes, offset + 4) : null;
    } else {
      throw new IllegalStateException("Both timestamp and bytes are empty");
    }
  }

  // offset should point to the start of decimal field
  private static Integer getTimezoneOffset(byte[] bytes, final int offset) {
    if (hasTimezoneOffset(bytes, offset)) {
      int pos = offset + WritableUtils.decodeVIntSize(bytes[offset]);
      // skip the 2nd VInt
      if (hasSecondVInt(bytes[offset])) {
        pos += WritableUtils.decodeVIntSize(bytes[pos]);
      }
      return readVInt(bytes, pos);
    }
    return null;
  }

  private static boolean hasTimezoneOffset(byte[] bytes, int offset) {
    int val = readVInt(bytes, offset);
    return (val >= 0 && (val & TIMEZONE_MASK) != 0) ||
        (val < 0 && (val & TIMEZONE_MASK) == 0);
  }

  /**
   * @return length of serialized TimestampWritable data. As a side effect, populates the internal
   *         byte array if empty.
   */
  int getTotalLength() {
    checkBytes();
    return getTotalLength(currentBytes, offset);
  }

  public static int getTotalLength(byte[] bytes, int offset) {
    int pos = offset + 4;
    if (hasDecimalOrSecondVInt(bytes[offset])) {
      boolean hasSecondVInt = hasSecondVInt(bytes[pos]);
      boolean hasTimezoneOffset = hasTimezoneOffset(bytes, pos);
      pos += WritableUtils.decodeVIntSize(bytes[pos]);
      if (hasSecondVInt) {
        pos += WritableUtils.decodeVIntSize(bytes[pos]);
      }
      if (hasTimezoneOffset) {
        pos += WritableUtils.decodeVIntSize(bytes[pos]);
      }
    }
    return pos - offset;
  }

  public HiveTimestamp getTimestamp() {
    if (timestampEmpty) {
      populateTimestamp();
    }
    return timestamp;
  }

  /**
   * Used to create copies of objects
   * @return a copy of the internal TimestampWritable byte[]
   */
  public byte[] getBytes() {
    checkBytes();

    int len = getTotalLength();
    byte[] b = new byte[len];

    System.arraycopy(currentBytes, offset, b, 0, len);
    return b;
  }

  /**
   * @return byte[] representation of TimestampWritable that is binary
   * sortable (7 bytes for seconds, 4 bytes for nanoseconds, 4 bytes for timezone offset)
   */
  public byte[] getBinarySortable() {
    byte[] b = new byte[BINARY_SORTABLE_LENGTH];
    int nanos = getNanos();
    // We flip the highest-order bit of the seven-byte representation of seconds to make negative
    // values come before positive ones.
    long seconds = getSeconds() ^ SEVEN_BYTE_LONG_SIGN_FLIP;
    sevenByteLongToBytes(seconds, b, 0);
    intToBytes(nanos, b, 7);
    Integer tzOffset = getTimezoneOffset();
    if (tzOffset == null) {
      tzOffset = HiveTimestamp.NULL_OFFSET;
    }
    intToBytes(tzOffset ^ DECIMAL_OR_SECOND_VINT_FLAG, b, 11);
    return b;
  }

  /**
   * Given a byte[] that has binary sortable data, initialize the internal
   * structures to hold that data
   * @param bytes the byte array that holds the binary sortable representation
   * @param binSortOffset offset of the binary-sortable representation within the buffer.
   */
  public void setBinarySortable(byte[] bytes, int binSortOffset) {
    // Flip the sign bit (and unused bits of the high-order byte) of the seven-byte long back.
    long seconds = readSevenByteLong(bytes, binSortOffset) ^ SEVEN_BYTE_LONG_SIGN_FLIP;
    int nanos = bytesToInt(bytes, binSortOffset + 7);
    int tzOffset = bytesToInt(bytes, binSortOffset + 11) ^ DECIMAL_OR_SECOND_VINT_FLAG;
    boolean hasTimezone = HiveTimestamp.isValidOffset(tzOffset);
    int firstInt = (int) seconds;
    boolean hasSecondVInt = seconds < 0 || seconds > Integer.MAX_VALUE;
    if (nanos != 0 || hasSecondVInt || hasTimezone) {
      firstInt |= DECIMAL_OR_SECOND_VINT_FLAG;
    } else {
      firstInt &= LOWEST_31_BITS_OF_SEC_MASK;
    }

    intToBytes(firstInt, internalBytes, 0);
    setNanosBytes(nanos, internalBytes, 4, hasSecondVInt, hasTimezone);
    int pos = 4;
    if (hasSecondVInt) {
      pos += WritableUtils.decodeVIntSize(internalBytes[pos]);
      LazyBinaryUtils.writeVLongToByteArray(internalBytes, pos, seconds >> 31);
    }

    if (hasTimezone) {
      pos += WritableUtils.decodeVIntSize(internalBytes[pos]);
      LazyBinaryUtils.writeVLongToByteArray(internalBytes, pos, tzOffset);
    }

    currentBytes = internalBytes;
    this.offset = 0;
  }

  /**
   * The data of TimestampWritable can be stored either in a byte[]
   * or in a Timestamp object. Calling this method ensures that the byte[]
   * is populated from the Timestamp object if previously empty.
   */
  private void checkBytes() {
    if (bytesEmpty) {
      // Populate byte[] from Timestamp
      populateBytes();
      offset = 0;
      currentBytes = internalBytes;
      bytesEmpty = false;
    }
  }

  /**
   *
   * @return double representation of the timestamp, accurate to nanoseconds
   */
  public double getDouble() {
    double seconds, nanos;
    if (bytesEmpty) {
      seconds = TimestampUtils.millisToSeconds(timestamp.getTime());
      nanos = timestamp.getNanos();
    } else {
      seconds = getSeconds();
      nanos = getNanos();
    }
    return seconds + nanos / 1000000000;
  }

  public static long getLong(Timestamp timestamp) {
    return timestamp.getTime() / 1000;
  }

  public void readFields(DataInput in) throws IOException {
    in.readFully(internalBytes, 0, 4);
    if (TimestampWritable.hasDecimalOrSecondVInt(internalBytes[0])) {
      in.readFully(internalBytes, 4, 1);
      int len = (byte) WritableUtils.decodeVIntSize(internalBytes[4]);
      if (len > 1) {
        in.readFully(internalBytes, 5, len - 1);
      }

      int pos = 4 + len;
      if (hasSecondVInt(internalBytes[4])) {
        // This indicates there is a second VInt containing the additional bits of the seconds
        // field.
        in.readFully(internalBytes, pos, 1);
        int secondVIntLen = (byte) WritableUtils.decodeVIntSize(internalBytes[pos]);
        if (secondVIntLen > 1) {
          in.readFully(internalBytes, pos + 1, secondVIntLen - 1);
        }
        pos += secondVIntLen;
      }

      if (hasTimezoneOffset(internalBytes, 4)) {
        in.readFully(internalBytes, pos, 1);
        int tzOffsetLen = WritableUtils.decodeVIntSize(internalBytes[pos]);
        if (tzOffsetLen > 1) {
          in.readFully(internalBytes, pos + 1, tzOffsetLen - 1);
        }
      }
    }
    currentBytes = internalBytes;
    this.offset = 0;
  }

  public void write(DataOutput out) throws IOException {
    checkBytes();
    out.write(currentBytes, offset, getTotalLength());
  }

  @Override
  public int compareTo(TimestampWritable t) {
    checkBytes();
    long s1 = this.getSeconds();
    long s2 = t.getSeconds();
    if (s1 == s2) {
      int n1 = this.getNanos();
      int n2 = t.getNanos();
      if (n1 == n2) {
        Integer tz1 = getTimezoneOffset();
        Integer tz2 = t.getTimezoneOffset();
        if (tz1 == null || tz2 == null) {
          if (tz1 != null) {
            return 1;
          }
          if (tz2 != null) {
            return -1;
          }
          return 0;
        }
        return tz1 - tz2;
      }
      return n1 - n2;
    } else {
      return s1 < s2 ? -1 : 1;
    }
  }

  @Override
  public boolean equals(Object o) {
    return compareTo((TimestampWritable) o) == 0;
  }

  @Override
  public String toString() {
    if (timestampEmpty) {
      populateTimestamp();
    }

    String timestampString = timestamp.toString();
    if (timestampString.length() > 19) {
      if (timestampString.substring(19, 21).compareTo(".0") == 0) {
        if (timestampString.length() == 21 || !Character.isDigit(timestampString.charAt(21))) {
          timestampString = timestampString.substring(0, 19) + timestampString.substring(21);
        }
      }
    }

    return timestampString;
  }

  @Override
  public int hashCode() {
    long seconds = getSeconds();
    seconds <<= 30;  // the nanosecond part fits in 30 bits
    seconds |= getNanos();
    Integer tzOffset = getTimezoneOffset();
    int hash = (int) ((seconds >>> 32) ^ seconds);
    if (tzOffset != null) {
      hash ^= tzOffset;
    }
    return hash;
  }

  private void populateTimestamp() {
    long seconds = getSeconds();
    int nanos = getNanos();
    timestamp.setTime(seconds * 1000);
    timestamp.setNanos(nanos);
    timestamp.setOffsetInMin(getTimezoneOffset());
  }

  /** Static methods **/

  /**
   * Gets seconds stored as integer at bytes[offset]
   * @param bytes
   * @param offset
   * @return the number of seconds
   */
  public static long getSeconds(byte[] bytes, int offset) {
    int lowest31BitsOfSecondsAndFlag = bytesToInt(bytes, offset);
    if (lowest31BitsOfSecondsAndFlag >= 0 ||  // the "has decimal or second VInt" flag is not set
        !hasSecondVInt(bytes[offset + 4])) {
      // The entire seconds field is stored in the first 4 bytes.
      return lowest31BitsOfSecondsAndFlag & LOWEST_31_BITS_OF_SEC_MASK;
    }

    // We compose the seconds field from two parts. The lowest 31 bits come from the first four
    // bytes. The higher-order bits come from the second VInt that follows the nanos field.
    return ((long) (lowest31BitsOfSecondsAndFlag & LOWEST_31_BITS_OF_SEC_MASK)) |
           (LazyBinaryUtils.readVLongFromByteArray(bytes,
               offset + 4 + WritableUtils.decodeVIntSize(bytes[offset + 4])) << 31);
  }

  public static int getNanos(byte[] bytes, int offset) {
    int val = readVInt(bytes, offset);
    if (val < 0) {
      val |= TIMEZONE_MASK;
      // This means there is a second VInt present that specifies additional bits of the timestamp.
      // The reversed nanoseconds value is still encoded in this VInt.
      val = -val - 1;
    } else {
      val &= ~TIMEZONE_MASK;
    }
    int len = (int) Math.floor(Math.log10(val)) + 1;

    // Reverse the value
    int tmp = 0;
    while (val != 0) {
      tmp *= 10;
      tmp += val % 10;
      val /= 10;
    }
    val = tmp;

    if (len < 9) {
      val *= Math.pow(10, 9 - len);
    }
    return val;
  }

  private static int readVInt(byte[] bytes, int offset) {
    VInt vInt = LazyBinaryUtils.threadLocalVInt.get();
    LazyBinaryUtils.readVInt(bytes, offset, vInt);
    return vInt.value;
  }

  /**
   * Writes the Timestamp's serialized value to the internal byte array.
   */
  private void populateBytes() {
    long millis = timestamp.getTime();
    int nanos = timestamp.getNanos();

    boolean hasTimezone = timestamp.hasTimezone();
    long seconds = TimestampUtils.millisToSeconds(millis);
    boolean hasSecondVInt = seconds < 0 || seconds > Integer.MAX_VALUE;
    int position = 4;
    boolean hasDecimal = setNanosBytes(nanos, internalBytes, position, hasSecondVInt, hasTimezone);

    int firstInt = (int) seconds;
    if (hasDecimal || hasSecondVInt || hasTimezone) {
      firstInt |= DECIMAL_OR_SECOND_VINT_FLAG;
    } else {
      firstInt &= LOWEST_31_BITS_OF_SEC_MASK;
    }
    intToBytes(firstInt, internalBytes, 0);

    if (hasSecondVInt) {
      position += WritableUtils.decodeVIntSize(internalBytes[position]);
      LazyBinaryUtils.writeVLongToByteArray(internalBytes, position, seconds >> 31);
    }

    if (hasTimezone) {
      position += WritableUtils.decodeVIntSize(internalBytes[position]);
      LazyBinaryUtils.writeVLongToByteArray(internalBytes, position,
          timestamp.getOffsetInMin());
    }
  }

  /**
   * Given an integer representing nanoseconds, write its serialized
   * value to the byte array b at offset
   *
   * @param nanos
   * @param b
   * @param offset
   * @return
   */
  private static boolean setNanosBytes(int nanos, byte[] b, int offset,
      boolean hasSecondVInt, boolean hasTimezone) {
    int decimal = 0;
    if (nanos != 0) {
      int counter = 0;
      while (counter < 9) {
        decimal *= 10;
        decimal += nanos % 10;
        nanos /= 10;
        counter++;
      }
    }

    if (hasSecondVInt || decimal != 0 || hasTimezone) {
      // We use the sign of the reversed-nanoseconds field to indicate that there is a second VInt
      // present.
      int toWrite = decimal;
      if (hasSecondVInt) {
        toWrite = -toWrite - 1;
      }
      // Decimal ranges in [-1000000000, 999999999]. Use the second MSB to indicate if
      // timezone is present.
      // if toWrite >= 0, second MSB is always 0, otherwise it's always 1
      if (hasTimezone) {
        if (toWrite >= 0) {
          toWrite |= TIMEZONE_MASK;
        } else {
          toWrite &= ~TIMEZONE_MASK;
        }
      }
      LazyBinaryUtils.writeVLongToByteArray(b, offset, toWrite);
    }
    return decimal != 0;
  }

  public HiveDecimal getHiveDecimal() {
    if (timestampEmpty) {
      populateTimestamp();
    }
    return getHiveDecimal(timestamp);
  }

  public static HiveDecimal getHiveDecimal(Timestamp timestamp) {
    // The BigDecimal class recommends not converting directly from double to BigDecimal,
    // so we convert through a string...
    Double timestampDouble = TimestampUtils.getDouble(timestamp);
    HiveDecimal result = HiveDecimal.create(timestampDouble.toString());
    return result;
  }


  /**
   * Converts the time in seconds or milliseconds to a timestamp.
   * @param time time in seconds or in milliseconds
   * @return the timestamp
   */
  public static Timestamp longToTimestamp(long time, boolean intToTimestampInSeconds) {
      // If the time is in seconds, converts it to milliseconds first.
      return new Timestamp(intToTimestampInSeconds ?  time * 1000 : time);
  }

  public static void setTimestamp(Timestamp t, byte[] bytes, int offset) {
    long seconds = getSeconds(bytes, offset);
    t.setTime(seconds * 1000);
    if (hasDecimalOrSecondVInt(bytes[offset])) {
      t.setNanos(getNanos(bytes, offset + 4));
    } else {
      t.setNanos(0);
    }

    Integer tzOffset = getTimezoneOffset(bytes, offset + 4);
    if (t instanceof HiveTimestamp) {
      ((HiveTimestamp) t).setOffsetInMin(tzOffset);
    } else {
      Preconditions.checkArgument(tzOffset == null);
    }
  }

  public static Timestamp createTimestamp(byte[] bytes, int offset) {
    Timestamp t = new HiveTimestamp(0);
    TimestampWritable.setTimestamp(t, bytes, offset);
    return t;
  }

  private static boolean hasDecimalOrSecondVInt(byte b) {
    return (b >> 7) != 0;
  }

  private static boolean hasSecondVInt(byte b) {
    return WritableUtils.isNegativeVInt(b);
  }

  private final boolean hasDecimalOrSecondVInt() {
    return hasDecimalOrSecondVInt(currentBytes[offset]);
  }

  /**
   * Writes <code>value</code> into <code>dest</code> at <code>offset</code>
   * @param value
   * @param dest
   * @param offset
   */
  private static void intToBytes(int value, byte[] dest, int offset) {
    dest[offset] = (byte) ((value >> 24) & 0xFF);
    dest[offset+1] = (byte) ((value >> 16) & 0xFF);
    dest[offset+2] = (byte) ((value >> 8) & 0xFF);
    dest[offset+3] = (byte) (value & 0xFF);
  }

  /**
   * Writes <code>value</code> into <code>dest</code> at <code>offset</code> as a seven-byte
   * serialized long number.
   */
  static void sevenByteLongToBytes(long value, byte[] dest, int offset) {
    dest[offset] = (byte) ((value >> 48) & 0xFF);
    dest[offset+1] = (byte) ((value >> 40) & 0xFF);
    dest[offset+2] = (byte) ((value >> 32) & 0xFF);
    dest[offset+3] = (byte) ((value >> 24) & 0xFF);
    dest[offset+4] = (byte) ((value >> 16) & 0xFF);
    dest[offset+5] = (byte) ((value >> 8) & 0xFF);
    dest[offset+6] = (byte) (value & 0xFF);
  }

  /**
   *
   * @param bytes
   * @param offset
   * @return integer represented by the four bytes in <code>bytes</code>
   *  beginning at <code>offset</code>
   */
  private static int bytesToInt(byte[] bytes, int offset) {
    return ((0xFF & bytes[offset]) << 24)
        | ((0xFF & bytes[offset+1]) << 16)
        | ((0xFF & bytes[offset+2]) << 8)
        | (0xFF & bytes[offset+3]);
  }

  static long readSevenByteLong(byte[] bytes, int offset) {
    // We need to shift everything 8 bits left and then shift back to populate the sign field.
    return (((0xFFL & bytes[offset]) << 56)
        | ((0xFFL & bytes[offset+1]) << 48)
        | ((0xFFL & bytes[offset+2]) << 40)
        | ((0xFFL & bytes[offset+3]) << 32)
        | ((0xFFL & bytes[offset+4]) << 24)
        | ((0xFFL & bytes[offset+5]) << 16)
        | ((0xFFL & bytes[offset+6]) << 8)) >> 8;
  }
}
