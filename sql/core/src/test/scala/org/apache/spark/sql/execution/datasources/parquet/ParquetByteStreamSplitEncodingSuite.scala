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
package org.apache.spark.sql.execution.datasources.parquet

import java.nio.ByteBuffer
import java.util.Random

import org.apache.parquet.bytes.{ByteBufferInputStream, DirectByteBufferAllocator}
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter
import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, FloatType}

/**
 * Read tests for vectorized Delta byte array  reader.
 * Translated from org.apache.parquet.column.values.delta.ByteStreamSplitValues*Test
 */
class ParquetByteStreamSplitEncodingSuite
    extends ParquetCompatibilityTest
    with SharedSparkSession {

  private var reader: VectorizedByteStreamSplitReader = _
  private var writableColumnVector: WritableColumnVector = _
  private var writer: ValuesWriter = _
  private var random: Random = _

  private def testReader(input: Array[Byte], expected: Array[Float]): Unit = {
    val length = expected.length
    reader = new VectorizedByteStreamSplitReaderFloat
    writableColumnVector = new OnHeapColumnVector(length, FloatType)
    reader.initFromPage(length, ByteBufferInputStream.wrap(ByteBuffer.wrap(input)))
    reader.readFloats(length, writableColumnVector, 0)
    for (i <- expected.indices) {
      assert(expected(i) === writableColumnVector.getFloat(i))
    }
  }

  test("single element (float)") {
    val byteData = Array(0x00.toByte, 0x00.toByte, 0x10.toByte, 0x40.toByte)
    testReader(byteData, Array[Float](2.25f))
  }

  test("small buffer (float)") {
    val byteData = Array(
      0x40.toByte,
      0x00.toByte,
      0x80.toByte,
      0x40.toByte,
      0x05.toByte,
      0x84.toByte,
      0xc5.toByte,
      0xbd.toByte,
      0x32.toByte,
      0xc2.toByte,
      0x41.toByte,
      0x42.toByte)
    val expectedValues = Array(-98.62548828125f, 23.62744140625f, 44.62939453125f)
    testReader(byteData, expectedValues)
  }

  test("random input (float)") {
    val rand = new Random(1337)
    val numElements = 256
    val byteData = new Array[Byte](numElements * 4)
    val values = new Array[Float](numElements)
    for (i <- 0 until numElements) {
      val f = rand.nextFloat * 1024.0f
      values(i) = f
      val fAsInt = java.lang.Float.floatToIntBits(f)
      byteData(i) = (fAsInt & 0xff).toByte
      byteData(numElements + i) = ((fAsInt >> 8) & 0xff).toByte
      byteData(numElements * 2 + i) = ((fAsInt >> 16) & 0xff).toByte
      byteData(numElements * 3 + i) = ((fAsInt >> 24) & 0xff).toByte
    }
    testReader(byteData, values)
  }

  test("extra reads") {
    val byteData = Array(0x00.toByte, 0x00.toByte, 0x10.toByte, 0x40.toByte)
    testReader(byteData, Array(2.25f))
    assertThrows[ParquetDecodingException] {
      reader.readFloats(1, writableColumnVector, 1)
    }
  }

  test("skip") {
    val byteData = new Array[Byte](16)
    for (i <- 0 until 16) {
      byteData(i) = 0xff.toByte
    }
    byteData(3) = 0x00.toByte
    byteData(7) = 0x00.toByte
    byteData(11) = 0x10.toByte
    byteData(15) = 0x40.toByte
    val length = 4
    reader = new VectorizedByteStreamSplitReaderFloat
    writableColumnVector = new OnHeapColumnVector(length, FloatType)
    reader.initFromPage(length, ByteBufferInputStream.wrap(ByteBuffer.wrap(byteData)))
    reader.skipFloats(3)
    reader.readFloats(1, writableColumnVector, 3)
    assert(2.25f === writableColumnVector.getFloat(3))
  }

  test("skip overflow") {
    val byteData = new Array[Byte](128)
    val length = 32
    reader = new VectorizedByteStreamSplitReaderFloat
    writableColumnVector = new OnHeapColumnVector(length, FloatType)
    reader.initFromPage(length, ByteBufferInputStream.wrap(ByteBuffer.wrap(byteData)))
    assertThrows[ParquetDecodingException] {
      reader.skipFloats(33)
    }
  }

  test("skip under flow") {
    val byteData = new Array[Byte](128)
    val length = 32
    reader = new VectorizedByteStreamSplitReaderFloat
    writableColumnVector = new OnHeapColumnVector(length, FloatType)
    reader.initFromPage(length, ByteBufferInputStream.wrap(ByteBuffer.wrap(byteData)))
    assertThrows[ParquetDecodingException] {
      reader.skipFloats(-1)
    }
  }

  private def testReader(data: Array[Byte], expected: Array[Double]): Unit = {
    val length = expected.length
    reader = new VectorizedByteStreamSplitReaderDouble
    writableColumnVector = new OnHeapColumnVector(length, DoubleType)
    reader.initFromPage(length, ByteBufferInputStream.wrap(ByteBuffer.wrap(data)))
    reader.readDoubles(length, writableColumnVector, 0)
    for (i <- expected.indices) {
      assert(expected(i) === writableColumnVector.getDouble(i))
    }
  }

  test("single element (double)") {
    val byteData = Array(
      0xfe.toByte,
      0xff.toByte,
      0xff.toByte,
      0x0d.toByte,
      0xa8.toByte,
      0x77.toByte,
      0xd2.toByte,
      0x40.toByte)
    testReader(byteData, Array[Double](18910.62585449218))
  }

  test("small buffer (double)") {
    val byteData = Array(
      0xe7.toByte,
      0x72.toByte,
      0xbe.toByte,
      0x09.toByte,
      0xa1.toByte,
      0xc1.toByte,
      0x0a.toByte,
      0x0a.toByte,
      0x17.toByte,
      0xd7.toByte,
      0x21.toByte,
      0x26.toByte,
      0x01.toByte,
      0xc7.toByte,
      0x53.toByte,
      0x0a.toByte,
      0x46.toByte,
      0x05.toByte,
      0x70.toByte,
      0xf3.toByte,
      0xe4.toByte,
      0x40.toByte,
      0xc0.toByte,
      0x3f.toByte)
    val expectedValues = Array(256.625449218, -78956.4455667788, 0.62565)
    testReader(byteData, expectedValues)
  }

  test("random input (double)") {
    val rand = new Random(6557)
    val numElements = 256
    val byteData = new Array[Byte](numElements * 8)
    val values = new Array[Double](numElements)
    for (i <- 0 until numElements) {
      val f = rand.nextDouble * 8192.0
      values(i) = f
      val fAsLong = java.lang.Double.doubleToLongBits(f)
      for (j <- 0 until 8) {
        byteData(numElements * j + i) = ((fAsLong >> (8 * j)) & 0xff).toByte
      }
    }
    testReader(byteData, values)
  }

  test("float read and write") { // Generate random data.
    random = new Random(1337)
    val numElements = 1024
    val values = new Array[Float](numElements)
    for (i <- 0 until numElements) {
      val f = random.nextFloat * 4096.0f
      values(i) = f
    }
    writer = new ByteStreamSplitValuesWriter.FloatByteStreamSplitValuesWriter(
      numElements * 4,
      numElements * 4,
      new DirectByteBufferAllocator)
    for (v <- values) {
      writer.writeFloat(v)
    }
    assert(numElements * 4 == writer.getBufferedSize)
    val input = writer.getBytes.toByteArray
    assert(numElements * 4 == input.size)
    testReader(input, values);
  }

  test("double read and write") {
    val rand = new Random(18990)
    val numElements = 1024
    val values = new Array[Double](numElements)
    for (i <- 0 until numElements) {
      val f = rand.nextDouble * 16384.0
      values(i) = f
    }
    val writer = new ByteStreamSplitValuesWriter.DoubleByteStreamSplitValuesWriter(
      numElements * 8,
      numElements * 8,
      new DirectByteBufferAllocator)
    for (v <- values) {
      writer.writeDouble(v)
    }
    assert(numElements * 8 == writer.getBufferedSize)
    val input = writer.getBytes.toByteArray
    assert(numElements * 8 == input.size)
    testReader(input, values)
  }

}
