/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * An implementation of the Parquet BYTE_STREAM_SPLIT decoder that supports the vectorized
 * interface.
 */
public class VectorizedByteStreamSplitReader extends ValuesReader
    implements VectorizedValuesReader {

  private final int elementSizeInBytes;
  private int indexInStream;
  private int valueCount;

  // Keep a reference to the underlying byte buffer
  private ByteBuffer underlyingBuffer;
  private final byte[] valueByteBuffer;

  public VectorizedByteStreamSplitReader(int elementSizeInBytes) {
    this.elementSizeInBytes = elementSizeInBytes;
    this.valueByteBuffer = new byte[elementSizeInBytes];
  }

  @Override
  public void initFromPage(/*unused*/int valueCount, ByteBufferInputStream in) throws IOException {
    Preconditions.checkArgument(valueCount >= 1,
        "Page must have at least one value, but it has " + valueCount);
    this.valueCount = valueCount;
    indexInStream = 0;
    // ByteBufferInputStream.slice does not implement the contract of ByteBuffer.slice correctly.
    // In particular, the position in the byte buffer in the slice is not zero as required, but is
    // the offset of the slice in the underlying buffer. Using the ByteBuffer api to get
    // specific elements within the slice will then return the wrong element unless you read
    // sequentially
    // If it had done this correctly, we could point to the slice corresponding to each split
    // stream and let the byte buffer do the math of getting us the correct element. But it
    // doesn't, so we must get the underlying buffer and do the math ourselves.
    // Luckily, it isn't too hard.
    // In addition, if the ByteBufferInputStream is a multi buffer input stream the slice
    // method is a _copy_ of the data if the length requested is larger than the size of the
    // current buffer. There is no getting around that.
    // TL;DR the line of code below may not always be a zero-copy operation
    underlyingBuffer = in.slice(valueCount * elementSizeInBytes);
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte readByte() {
    throw new UnsupportedOperationException();
  }

  @Override
  public short readShort() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Binary readBinary(int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBooleans(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBytes(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readShorts(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readIntegers(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readIntegersWithRebase(int total, WritableColumnVector c, int rowId,
      boolean failIfRebase) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readUnsignedIntegers(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readUnsignedLongs(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readLongs(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readLongsWithRebase(int total, WritableColumnVector c, int rowId,
      boolean failIfRebase) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readFloats(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readDoubles(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBinary(int total, WritableColumnVector c, int rowId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipBooleans(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipBytes(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipShorts(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipIntegers(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipLongs(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipFloats(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipDoubles(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipBinary(int total) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void skipFixedLenByteArray(int total, int len) {
    throw new UnsupportedOperationException();
  }

  protected void readValues(int total, WritableColumnVector c, int rowId,
      SplitByteStreamOutputWriter outputWriter) {
    if (total < 0 || indexInStream + total > valueCount) {
      String errorMessage = String.format(
          "Cannot read this many elements. Current index: %d. "
              + "Skip %d. Total number of elements: %d",
          indexInStream, total, valueCount);
      throw new ParquetDecodingException(errorMessage);
    }
    for (int i = 0; i < total; i++) {
      outputWriter.write(c, rowId + i, getNextElement());
    }
  }

  private byte[] getNextElement() throws ParquetDecodingException {
    for (int i = 0; i < elementSizeInBytes; ++i) {
      valueByteBuffer[i] = underlyingBuffer.get(i * valueCount + indexInStream);
    }
    ++indexInStream;
    return valueByteBuffer;
  }

  protected void skipValues(int total) {
    if (total < 0 || indexInStream + total > valueCount) {
      String errorMessage = String.format(
          "Cannot skip this many elements. Current index: %d. " +
              "Skip %d. Total number of elements: %d",
          indexInStream, total, valueCount);
      throw new ParquetDecodingException(errorMessage);
    }
    indexInStream += total;
  }

}
