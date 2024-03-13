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

import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.hadoop.ParquetMetricsCallback;
import org.apache.spark.sql.connector.metric.CustomTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDecodeTimeTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDecompressSizeTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDecompressThroughputTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetDecompressTimeTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetLoadRowGroupTimeTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetReadSizeTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetReadThroughputTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetReadTimeTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetRowGroupsTaskMetric;
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetSeekTimeTaskMetric;
import org.apache.spark.sql.execution.metric.CustomMetrics$;
import org.apache.spark.sql.execution.metric.SQLMetric;

public class ParquetMetricsCallbackImpl implements ParquetMetricsCallback {

    private final Map<String, SQLMetric> metrics;
    private final Map<String, Long> metricValues = new HashMap<>();

    public ParquetMetricsCallbackImpl(Map<String, SQLMetric> metrics) {
        this.metrics = metrics;
    }

    @Override
    public void setValueInt(String metricName, int value) {
        SQLMetric metric;
        if (metrics != null && (metric = metrics.get(metricName)) != null && !metric.metricType()
                .startsWith(CustomMetrics$.MODULE$.V2_CUSTOM())) {
            metric.add(value);
        } else {
            // Because custom metrics do not support floating point metric values
            updateMetricValue(metricName, value);
        }
    }

    @Override
    public void setValueLong(String metricName, long value) {
        SQLMetric metric;
        if (metrics != null && (metric = metrics.get(metricName)) != null && !metric.metricType()
                .startsWith(CustomMetrics$.MODULE$.V2_CUSTOM())) {
            metric.add(value);
        } else {
            // Because custom metrics do not support floating point metric values
            updateMetricValue(metricName, value);
        }
    }

    @Override
    public void setValueFloat(String metricName, float value) {
        SQLMetric metric;
        if (metrics != null && (metric = metrics.get(metricName)) != null && !metric.metricType()
                .startsWith(CustomMetrics$.MODULE$.V2_CUSTOM())) {
            metric.set(value);
        } else {
            // Because custom metrics do not support floating point metric values
            updateMetricValue(metricName, (long) value);
        }
    }

    @Override
    public void setValueDouble(String metricName, double value) {
        SQLMetric metric;
        if (metrics != null && (metric = metrics.get(metricName)) != null && !metric.metricType()
                .startsWith(CustomMetrics$.MODULE$.V2_CUSTOM())) {
            metric.set(value);
        } else {
            // Because custom metrics do not support floating point metric values
            updateMetricValue(metricName, (long) value);
        }
    }

    @Override
    public void setDuration(String s, long l) {
       setValueLong(s, l);
    }

    private void updateMetricValue(String metricName, long value) {
        if (metricValues.containsKey(metricName)) {
            metricValues.put(metricName, metricValues.get(metricName) + value);
        } else {
            metricValues.put(metricName, value);
        }
    }

    private long getMetricValue(String metricName) {
        return metricValues.getOrDefault(metricName, 0L);
    }

    public CustomTaskMetric[] currentMetricsValues() {

        return new CustomTaskMetric[]{
                new ParquetRowGroupsTaskMetric(getMetricValue("ParquetRowGroups")),
                new ParquetDecodeTimeTaskMetric(getMetricValue("ParquetDecodeTime")),
                new ParquetLoadRowGroupTimeTaskMetric(getMetricValue("ParquetLoadRowGroupTime")),
                new ParquetReadTimeTaskMetric(getMetricValue("ReadTime")),
                new ParquetSeekTimeTaskMetric(getMetricValue("SeekTime")),
                new ParquetReadSizeTaskMetric(getMetricValue("ReadSize")),
                new ParquetReadThroughputTaskMetric(getMetricValue("ReadThroughput")),
                new ParquetDecompressTimeTaskMetric(getMetricValue("DecompressTime")),
                new ParquetDecompressSizeTaskMetric(getMetricValue("DecompressSize")),
                new ParquetDecompressThroughputTaskMetric(getMetricValue("DecompressThroughput"))
        };
    }

}
