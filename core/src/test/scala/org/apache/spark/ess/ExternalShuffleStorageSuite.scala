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
package org.apache.spark.ess

import java.io.{DataOutputStream, File, FileOutputStream}

import scala.concurrent.duration._

import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.Eventually.{eventually, interval, timeout}

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SparkException, SparkFunSuite, TestUtils}
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.ess.ExternalShuffleStorage.{cleanUp, deleteObject, doesObjectExist, getAppIdPrefix, putObject}
import org.apache.spark.internal.config._
import org.apache.spark.launcher.SparkLauncher.{EXECUTOR_MEMORY, SPARK_MASTER}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.scheduler.cluster.StandaloneSchedulerBackend
import org.apache.spark.storage.ShuffleBlockId
import org.apache.spark.util.Utils.{createTempDir, tryWithResource}

class ExternalShuffleStorageSuite
  extends SparkFunSuite with LocalSparkContext with PrivateMethodTester {

  private val conf = ExternalShuffleStorage.enableExternalShuffleStorage(new SparkConf(false))
    .set(SPARK_SHUFFLE_EXTERNAL_STORAGE_BACKEND, "efs")
    .set(SPARK_SHUFFLE_EXTERNAL_STORAGE_BUCKET, createTempDir().getAbsolutePath)

  def getSparkConf(initialExecutor: Int = 1, minExecutor: Int = 1): SparkConf = {
    conf
      .setAppName(getClass.getName)
      .set(SPARK_MASTER, s"local-cluster[$initialExecutor,1,1024]")
      .set(EXECUTOR_MEMORY, "1g")
      .set(UI.UI_ENABLED, false)
      .set(DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED, true)
      .set(DYN_ALLOCATION_INITIAL_EXECUTORS, initialExecutor)
      .set(DYN_ALLOCATION_MIN_EXECUTORS, minExecutor)
  }

  test("External shuffle storage should show warnings for invalid confs") {
    val errorMsg = s"should be used only when ${DYN_ALLOCATION_ENABLED.key} and " +
      s"${Worker.WORKER_DECOMMISSION_ENABLED.key} and " +
      s"${STORAGE_DECOMMISSION_ENABLED.key} and " +
      s"${STORAGE_SHUFFLE_DECOMMISSION_ENABLED.key} and " +
      s"${SPARK_SHUFFLE_EXTERNAL_STORAGE_ENABLED.key} are true"

    // Every settings are correctly enabled.
    ExternalShuffleStorage.validateSettings(conf)

    // Since the user doesn't try to enable external storage, validation succeeds as no-op
    val conf1 = conf.clone.set(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENABLED, false)
    ExternalShuffleStorage.validateSettings(conf1)

    val conf2 = conf.clone.set(DYN_ALLOCATION_ENABLED, false)
    val e2 = intercept[SparkException] { ExternalShuffleStorage.validateSettings(conf2) }
    assert(e2.getMessage.contains(errorMsg))

    val conf3 = conf.clone.set(DYN_ALLOCATION_ENABLED, false)
    val e3 = intercept[SparkException] { ExternalShuffleStorage.validateSettings(conf3) }
    assert(e3.getMessage.contains(errorMsg))
  }

  test("Custom shuffle manager is not allowed") {
    val conf1 = conf.clone.set(SHUFFLE_MANAGER, "customer_shuffle")
    val e = intercept[SparkException] { ExternalShuffleStorage.validateSettings(conf1) }
    assert(e.getMessage.contains("Custom shuffle manager, customer_shuffle, is not supported"))
  }

  test("Speculative execution is not allowed") {
    val conf1 = conf.clone.set(SPECULATION_ENABLED, true)
    val e = intercept[SparkException] { ExternalShuffleStorage.validateSettings(conf1) }
    assert(e.getMessage.contains("spark.speculation should be disabled"))
  }

  test("Manage lifecycle of external storage location") {
    val conf = this.conf.clone
      .setAppName(getClass.getName)
      .setMaster("local-cluster[1,1,1024]")
    sc = new SparkContext(conf)
    val prefix = getAppIdPrefix(sc.getConf)
    assert(doesObjectExist(conf, prefix + "_start"))
    LocalSparkContext.withSpark(sc) { sc =>
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)
    }
    assert(!doesObjectExist(conf, prefix + "_start"))
    assert(!doesObjectExist(conf, prefix))
  }

  test("StorageShim API wrappers") {
    val appId = getClass.getSimpleName
    val file = File.createTempFile("tmp", ".tmp")

    val index1 = s"$appId/shuffle_0_0_0.index"
    val data1 = s"$appId/shuffle_0_0_0.data"
    val index2 = s"$appId/shuffle_0_1_0.index"
    val index3 = s"$appId/shuffle_1_2_0.index"
    val data3 = s"$appId/shuffle_1_2_0.data"

    putObject(conf, index1, file)
    putObject(conf, data1, file)
    assert(doesObjectExist(conf, index1))
    assert(doesObjectExist(conf, data1))

    deleteObject(conf, data1)
    assert(doesObjectExist(conf, index1))
    assert(!doesObjectExist(conf, data1))

    // Clean up an app
    cleanUp(conf, appId)
    assert(!doesObjectExist(conf, data1))

    // Clean up a shuffle
    putObject(conf, index1, file)
    putObject(conf, index2, file)
    putObject(conf, index3, file)
    cleanUp(conf, s"$appId/shuffle_0_")
    assert(!doesObjectExist(conf, index1))
    assert(!doesObjectExist(conf, index2))
    assert(doesObjectExist(conf, index3))

    // Clean up a file
    cleanUp(conf, data3)
    assert(!doesObjectExist(conf, data3))
  }

  test("Shuffle block APIs") {
    val appId = getClass.getSimpleName
    val conf = this.conf.clone.set("spark.app.id", appId)
    val indexFile = File.createTempFile("index", ".tmp")
    tryWithResource(new FileOutputStream(indexFile)) { fos =>
      tryWithResource(new DataOutputStream(fos)) { dos =>
        dos.writeLong(0)
        dos.writeLong(4)
      }
    }

    val dataFile = File.createTempFile("data", ".tmp")
    tryWithResource(new FileOutputStream(dataFile)) { fos =>
      tryWithResource(new DataOutputStream(fos)) { dos =>
        dos.writeLong(0)
      }
    }

    val index1 = s"$appId/shuffle_0_0_0.index"
    val data1 = s"$appId/shuffle_0_0_0.data"
    val index2 = s"$appId/shuffle_1_0_0.index"
    val data2 = s"$appId/shuffle_1_0_0.data"

    putObject(conf, index1, indexFile)
    putObject(conf, data1, dataFile)
    val read = PrivateMethod[ManagedBuffer](Symbol("read"))
    ExternalShuffleStorage invokePrivate read(conf, ShuffleBlockId(0, 0, 0))

    putObject(conf, index2, indexFile)
    putObject(conf, data2, dataFile)
    ExternalShuffleStorage.cleanUpShuffle(conf, 1)
    assert(!doesObjectExist(conf, index2))
    assert(!doesObjectExist(conf, data2))
    assert(doesObjectExist(conf, index1))
    assert(doesObjectExist(conf, data1))

    ExternalShuffleStorage.cleanUpAll(conf)
    assert(!doesObjectExist(conf, index1))
    assert(!doesObjectExist(conf, data1))
  }

  test("Upload from all decommissioned executors") {
    sc = new SparkContext(getSparkConf(2, 2))
    val conf = sc.getConf
    val prefix = getAppIdPrefix(conf)
    val files = Seq("shuffle_0_0_0.index", "shuffle_0_0_0.data")
    withSpark(sc) { sc =>
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
      val rdd1 = sc.parallelize(1 to 10, 10)
      val rdd2 = rdd1.map(x => (x % 2, 1))
      val rdd3 = rdd2.reduceByKey(_ + _)
      assert(rdd3.count() === 2)

      // Decommission all
      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().foreach(sched.decommissionExecutor)

      // Uploading is not started yet.
      files.foreach { key => assert(!doesObjectExist(conf, prefix + key)) }

      // Uploading is completed on decommissioned executors
      eventually(timeout(10.seconds), interval(1.seconds)) {
        files.foreach { key => assert(doesObjectExist(conf, prefix + key), key) }
      }

      // All executors are still alive.
      assert(sc.getExecutorIds().size == 2)
    }
  }

  test("Upload multi stages") {
    sc = new SparkContext(getSparkConf())
    val conf = sc.getConf
    val prefix = getAppIdPrefix(conf)
    val files = Seq(
      "shuffle_0_0_0.index", "shuffle_0_0_0.data",
      "shuffle_0_1_0.index", "shuffle_0_1_0.data",
      "shuffle_1_4_0.index", "shuffle_1_4_0.data",
      "shuffle_1_5_0.index", "shuffle_1_5_0.data")
    withSpark(sc) { sc =>
      TestUtils.waitUntilExecutorsUp(sc, 1, 60000)
      val rdd1 = sc.parallelize(1 to 10, 2)
      val rdd2 = rdd1.map(x => (x % 2, 1))
      val rdd3 = rdd2.reduceByKey(_ + _)
      val rdd4 = rdd3.sortByKey()
      assert(rdd4.count() === 2)
      files.foreach { key => assert(!doesObjectExist(conf, prefix + key), key) }

      // Decommission all
      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().foreach(sched.decommissionExecutor)

      eventually(timeout(10.seconds), interval(1.seconds)) {
        files.foreach { key => assert(doesObjectExist(conf, prefix + key), key) }
      }
    }
  }

  test("Newly added executors should access old data from remote storage") {
    sc = new SparkContext(getSparkConf(2, 0))

    val conf = sc.getConf
    val prefix = getAppIdPrefix(conf)
    withSpark(sc) { sc =>
      TestUtils.waitUntilExecutorsUp(sc, 2, 60000)
      val rdd1 = sc.parallelize(1 to 10, 2)
      val rdd2 = rdd1.map(x => (x % 2, 1))
      val rdd3 = rdd2.reduceByKey(_ + _)
      assert(rdd3.collect() === Array((0, 5), (1, 5)))

      // Decommission all
      val sched = sc.schedulerBackend.asInstanceOf[StandaloneSchedulerBackend]
      sc.getExecutorIds().foreach(sched.decommissionExecutor)

      // Make it sure that external storage are ready
      eventually(timeout(10.seconds), interval(1.seconds)) {
        Seq(
          "shuffle_0_0_0.index", "shuffle_0_0_0.data",
          "shuffle_0_1_0.index", "shuffle_0_1_0.data").foreach { key =>
          assert(doesObjectExist(conf, prefix + key))
        }
      }

      // Since the data is safe, force to shrink down to zero executor
      sc.getExecutorIds().foreach { id =>
        sched.killExecutor(id)
      }
      eventually(timeout(20.seconds), interval(1.seconds)) {
        assert(sc.getExecutorIds().isEmpty)
      }

      // Dynamic allocation will start new executors
      assert(rdd3.collect() === Array((0, 5), (1, 5)))
      assert(rdd3.sortByKey().count() == 2)
      assert(sc.getExecutorIds().nonEmpty)
    }
  }
}

