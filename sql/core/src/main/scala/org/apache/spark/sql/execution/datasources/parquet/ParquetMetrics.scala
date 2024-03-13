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

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

trait ParquetMetrics {
  private var metrics: Map[String, SQLMetric] = _

  def initOrGetMetrics(sparkContext: SparkContext): Map[String, SQLMetric] = {
    if (metrics == null) {
      metrics =
        Map(
          "ParquetRowGroups" -> SQLMetrics.createMetric(sparkContext,
            "number of Parquet row groups read"),
          "ParquetDecodeTime" -> SQLMetrics.createNanoTimingMetric(
            sparkContext,
            "time spent in Parquet column decoding"),
          "ParquetLoadRowGroupTime" -> SQLMetrics.createNanoTimingMetric(
            sparkContext,
            "time spent in loading Parquet row groups"),
          "ReadTime" -> SQLMetrics.createNanoTimingMetric(
            sparkContext,
            "time spent in reading Parquet file from storage"),
          "SeekTime" -> SQLMetrics.createNanoTimingMetric(
            sparkContext,
            "time spent in seek when reading Parquet file from storage"),
          "ReadSize" -> SQLMetrics.createSizeMetric(
            sparkContext,
            "read size when reading Parquet file from storage (MB)"),
          "ReadThroughput" -> SQLMetrics.createAverageMetric(
            sparkContext,
            "read throughput when reading Parquet file from storage (MB/sec)"),
          "DecompressTime" -> SQLMetrics.createNanoTimingMetric(
            sparkContext,
            "time spent in block decompression"),
          "DecompressSize" -> SQLMetrics.createSizeMetric(
            sparkContext,
            "block decompress ssize (MB)"),
          "DecompressThroughput" -> SQLMetrics.createAverageMetric(
            sparkContext,
            "block decompress throughput (MB/sec)")
        )
    }
    metrics
  }
}
