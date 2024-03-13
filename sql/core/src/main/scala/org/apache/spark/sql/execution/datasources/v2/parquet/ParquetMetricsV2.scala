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

package org.apache.spark.sql.execution.datasources.v2.parquet

import java.text.NumberFormat
import java.util
import java.util.Locale

import scala.concurrent.duration.DurationLong

import org.apache.spark.sql.connector.metric.{CustomAvgMetric, CustomFileTaskMetric, CustomMetric, CustomSumMetric}
import org.apache.spark.util.Utils


// See SQLMetrics.stringValue for corresponding implementation of aggregateTaskMetrics for built-in
// metrics
abstract class ParquetAverageMetric extends CustomMetric {

  private def toNumberFormat(value: Long): String = {
    val numberFormat = NumberFormat.getNumberInstance(Locale.US)
    numberFormat.format(value.toDouble / ParquetMetricsV2.baseForAvgMetric)
  }

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {

    val validValues = taskMetrics.filter(_ > 0)
    // When there are only 1 metrics value (or None), no need to display max/min/median. This is
    // common for driver-side SQL metrics.
    if (validValues.length <= 1) {
      toNumberFormat(validValues.headOption.getOrElse(0))
    } else {
      val Seq(min, med, max) = {
        util.Arrays.sort(validValues)
        Seq(
          toNumberFormat(validValues(0)),
          toNumberFormat(validValues(validValues.length / 2)),
          toNumberFormat(validValues(validValues.length - 1)))
      }
      s"${ParquetMetricsV2.METRICS_NAME_SUFFIX}:\n($min, $med, $max)"
    }
  }
}

abstract class ParquetSizeMetric extends CustomMetric {
  protected val strFormat: Long => String =
    Utils.bytesToString

  override def aggregateTaskMetrics(taskMetrics: Array[Long]): String = {

    val validValues = taskMetrics.filter(_ >= 0)
    // When there are only 1 metrics value (or None), no need to display max/min/median. This is
    // common for driver-side SQL metrics.
    if (validValues.length <= 1) {
      strFormat(validValues.headOption.getOrElse(0))
    } else {
      val Seq(sum, min, med, max) = {
        util.Arrays.sort(validValues)
        Seq(
          strFormat(validValues.sum),
          strFormat(validValues(0)),
          strFormat(validValues(validValues.length / 2)),
          strFormat(validValues(validValues.length - 1)))
      }
      s"total ${ParquetMetricsV2.METRICS_NAME_SUFFIX}:\n $sum ($min, $med, $max)"
    }
  }
}

abstract class ParquetTimingMetric extends ParquetSizeMetric {
  override  val strFormat: Long => String =
    Utils.msDurationToString
}

abstract class ParquetNSTimingMetric extends ParquetSizeMetric {
  override val strFormat: Long => String =
     duration => Utils.msDurationToString(duration.nanos.toMillis)
}

abstract class ParquetTaskMetricV2(initialValue: Long) extends CustomFileTaskMetric {
  protected var metricValue: Long = initialValue

  override def value(): Long = metricValue

  override def update(addValue: Long): Unit = metricValue = metricValue + addValue
}

class ParquetRowGroupsMetric extends CustomSumMetric {
  override def name(): String = "ParquetRowGroups"
  override def description(): String = "number of Parquet row groups read"
}

class ParquetRowGroupsTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "ParquetRowGroups"
}

class ParquetDecodeTimeMetric extends ParquetNSTimingMetric{
  override def name(): String = "ParquetDecodeTime"
  override def description: String = "time spent in Parquet column decoding"
}

class ParquetDecodeTimeTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "ParquetDecodeTime"
}

class ParquetLoadRowGroupTimeMetric extends ParquetNSTimingMetric {
  override def name(): String = "ParquetLoadRowGroupTime"
  override def description(): String = "time spent in loading Parquet row groups"
}

class ParquetLoadRowGroupTimeTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "ParquetLoadRowGroupTime"
}

class ParquetReadTimeMetric extends ParquetNSTimingMetric {
  override def name(): String = "ReadTime"
  override def description(): String = "time spent in reading Parquet file from storage"
}

class ParquetReadTimeTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "ReadTime"
}

class ParquetSeekTimeMetric extends ParquetNSTimingMetric {
  override def name(): String = "SeekTime"
  override def description(): String = "time spent in seek when reading Parquet file from storage"
}

class ParquetSeekTimeTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "SeekTime"
}

class ParquetReadSizeMetric extends ParquetSizeMetric {
  override def name(): String = "ReadSize"
  override def description(): String = "read size when reading Parquet file from storage (MB)"
}

class ParquetReadSizeTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "ReadSize"
}

class ParquetReadThroughputMetric extends CustomAvgMetric {
  override def name(): String = "ReadThroughput"
  override def description(): String =
    "read throughput when reading Parquet file from storage (MB/sec)"
}

class ParquetReadThroughputTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "ReadThroughput"
}

class ParquetDecompressTimeMetric extends ParquetNSTimingMetric {
  override def name(): String = "DecompressTime"
  override def description(): String = "time spent in block decompression"
}

class ParquetDecompressTimeTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "DecompressTime"
}

class ParquetDecompressSizeMetric extends ParquetSizeMetric {
  override def name(): String = "DecompressSize"
  override def description(): String = "block decompress size (MB)"
}

class ParquetDecompressSizeTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "DecompressSize"
}

class ParquetDecompressThroughputMetric extends CustomAvgMetric {
  override def name(): String = "DecompressThroughput"
  override def description(): String = "block decompress throughput (MB/sec)"
}

class ParquetDecompressThroughputTaskMetric(value: Long) extends ParquetTaskMetricV2(value) {
  override def name(): String = "DecompressThroughput"
}

object ParquetMetricsV2 {

  val METRICS_NAME_SUFFIX = "(min, med, max)"
  val baseForAvgMetric = 10

  val rowGroupsMetric = new ParquetRowGroupsMetric

  val metricByName = Map(
    "ParquetRowGroups" -> rowGroupsMetric

  )
  val metrics: Array[CustomMetric] = Array(
    rowGroupsMetric,
    new ParquetDecodeTimeMetric,
    new ParquetLoadRowGroupTimeMetric,
    new ParquetReadTimeMetric,
    new ParquetSeekTimeMetric,
    new ParquetReadSizeMetric,
    new ParquetReadThroughputMetric,
    new ParquetDecompressTimeMetric,
    new ParquetDecompressSizeMetric,
    new ParquetDecompressThroughputMetric
  )
}
