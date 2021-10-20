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

import static org.apache.spark.sql.types.DataTypes.IntegerType;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

/**
 * An implementation of the Parquet DELTA_LENGTH_BYTE_ARRAY decoder that supports the vectorized
 * interface.
 */
public class VectorizedDeltaLengthByteArrayReader extends ValuesReader implements
    VectorizedValuesReader {

  private int valueCount;
  private final VectorizedDeltaBinaryPackedReader lengthReader =
      new VectorizedDeltaBinaryPackedReader();
  private ByteBufferInputStream in;
  private WritableColumnVector lengthsVector;
  private int currentRow = 0;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    this.valueCount = valueCount;
    lengthReader.initFromPage(valueCount, in);
    lengthsVector = new OnHeapColumnVector(valueCount, IntegerType);
    lengthReader.readIntegers(valueCount, lengthsVector, 0);
    this.in = in.remainingStream();
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
    if (total == 0) {
      return;
    }
    ByteBuffer buffer;
    int length = lengthsVector.getInt(rowId);
    ByteBufferOutputWriter outputWriter;
    // read one value and use it to determine the type of the underlying buffer, then use the
    // appropriate output writer to write to the vector
    try {
      buffer = in.slice(length);
      if (buffer.hasArray()) {
        outputWriter = ByteBufferOutputWriter::writeArrayByteBuffer;
      } else {
        outputWriter = ByteBufferOutputWriter::copyWriteByteBuffer;
      }
      outputWriter.write(c, rowId, buffer, length);
      currentRow++;
    } catch (EOFException e) {
      throw new ParquetDecodingException("Failed to read " + length + " bytes");
    }
    for (int i = 1; i < total; i++) {
      length = lengthsVector.getInt(rowId + i);
      try {
        buffer = in.slice(length);
      } catch (EOFException e) {
        throw new ParquetDecodingException("Failed to read " + length + " bytes");
      }
      outputWriter.write(c, rowId+i, buffer, length);
      currentRow++;
    }
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
    if (total == 0) {
      return;
    }
    int length;
    for (int i = 0; i < total; i++) {
      length = lengthsVector.getInt(currentRow + i);
      int remaining = length;
      while (remaining > 0) {
        remaining -= in.skip(length);
      }
    }
    currentRow += total;
  }

  @Override
  public void skipFixedLenByteArray(int total, int len) {
    throw new UnsupportedOperationException();
  }

}
