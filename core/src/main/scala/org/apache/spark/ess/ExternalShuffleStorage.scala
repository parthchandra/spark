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

import java.io._
import java.util.concurrent.TimeUnit

import org.apache.commons.io.IOUtils.toByteArray
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.shuffle.{IndexShuffleBlockResolver, ShuffleBlockInfo, ShuffleBlockResolver}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId, BlockManagerMaster, ShuffleBlockBatchId, ShuffleBlockId, ShuffleDataBlockId, ShuffleIndexBlockId, StorageLevel}
import org.apache.spark.util.{ThreadUtils, Utils}


/**
 * The main business logic for external shuffle storage.
 */
object ExternalShuffleStorage extends Logging {

  /** We use one block manager id as a place holder. */
  val EXTERNAL_BLOCK_MANAGER_ID: BlockManagerId =
    BlockManagerId("external", "remote", 9000, None)

  /** An executor to delete external files asynchronously. */
  private val deleteExecutor = ThreadUtils.newDaemonFixedThreadPool(1, "ess-delete")

  def getAppIdPrefix(conf: SparkConf): String = {
    conf.get("spark.app.id") + conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_SEPARATOR)
  }

  def isEnabled(conf: SparkConf): Boolean = {
    conf.get(DYN_ALLOCATION_ENABLED) &&
      conf.get(DECOMMISSION_ENABLED) &&
      conf.get(STORAGE_DECOMMISSION_ENABLED) &&
      conf.get(STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED) &&
      conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENABLED)
  }

  def isDisabled(conf: SparkConf): Boolean = !isEnabled(conf)

  private def withCheck(conf: SparkConf)(f: => Unit): Unit = if (isEnabled(conf)) f
  private def withCheckOrFalse(conf: SparkConf)(f: => Boolean): Boolean =
    if (isEnabled(conf)) f else false
  private def withCheckOrException[T](conf: SparkConf)(f: => T): T =
    if (isEnabled(conf)) f else throw new UnsupportedOperationException("Not initialized")
  private def setFileSystemIfNeeded(conf: SparkConf) = {
    if (conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BACKEND).equals("hdfs") &&
      StorageShim.getFileSystem().isEmpty) {
      StorageShim.setFileSystem(
        FileSystem.get(new Path(conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENDPOINT)).toUri,
          SparkHadoopUtil.get.newConfiguration(conf)))
    }
  }

  // --------------------------------------------------------------------------
  // Wrapper functions for StorageShim API
  // --------------------------------------------------------------------------
  def putObject(conf: SparkConf, key: String, file: File): Unit = withCheck(conf) {
    val backend = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BACKEND)
    val accessKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ACCESS_KEY)
    val secretKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_SECRET_KEY)
    val endpoint = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENDPOINT)
    val bucket = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BUCKET)
    setFileSystemIfNeeded(conf)
    StorageShim.putObject(backend, accessKey, secretKey, endpoint, bucket, key, file)
  }

  def deleteObject(conf: SparkConf, key: String): Unit = withCheck(conf) {
    val backend = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BACKEND)
    val accessKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ACCESS_KEY)
    val secretKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_SECRET_KEY)
    val endpoint = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENDPOINT)
    val bucket = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BUCKET)
    setFileSystemIfNeeded(conf)
    StorageShim.deleteObject(backend, accessKey, secretKey, endpoint, bucket, key)
  }

  def doesObjectExist(conf: SparkConf, key: String): Boolean = withCheckOrFalse(conf) {
    val backend = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BACKEND)
    val accessKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ACCESS_KEY)
    val secretKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_SECRET_KEY)
    val endpoint = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENDPOINT)
    val bucket = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BUCKET)
    setFileSystemIfNeeded(conf)
    StorageShim.doesObjectExist(backend, accessKey, secretKey, endpoint, bucket, key)
  }

  def getObject(conf: SparkConf, key: String, start: Long, end: Long)
    : InputStream = withCheckOrException(conf) {
    val backend = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BACKEND)
    val accessKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ACCESS_KEY)
    val secretKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_SECRET_KEY)
    val endpoint = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENDPOINT)
    val bucket = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BUCKET)
    setFileSystemIfNeeded(conf)
    StorageShim.getObject(backend, accessKey, secretKey, endpoint, bucket, key, start, end)
  }

  def cleanUp(conf: SparkConf, prefix: String): Unit = withCheck(conf) {
    val backend = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BACKEND)
    val accessKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ACCESS_KEY)
    val secretKey = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_SECRET_KEY)
    val endpoint = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENDPOINT)
    val bucket = conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_BUCKET)
    setFileSystemIfNeeded(conf)
    StorageShim.cleanUp(backend, accessKey, secretKey, endpoint, bucket, prefix)
  }

  // --------------------------------------------------------------------------
  // Main APIs for external shuffle storage
  // Manage the lifecycle of the external storage prefix.
  // 1. Create the first file: `appId/_start` as a canary test for the external storage.
  //    This prevents failures in the middle of job execution due to inaccessible external storage.
  // 2. Check the configuration validity.
  //    This prevents data loss due to users' misconfiguration.
  // 3. Clean up all files starting with `appId` during shutdown.
  // --------------------------------------------------------------------------

  /** Shutdown executors */
  def shutdown(): Unit = {
    logInfo("Shutdown executor")
    deleteExecutor.shutdownNow()
    deleteExecutor.awaitTermination(10, TimeUnit.SECONDS);
  }

  /** Validate configuration to give an early warning. */
  def validateSettings(conf: SparkConf): Unit = {
    if (!conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENABLED)) return

    if (isDisabled(conf)) {
      throw new SparkException(s"${getClass.getName} should be used only when " +
        s"${DYN_ALLOCATION_ENABLED.key} and " +
        s"${DECOMMISSION_ENABLED.key} and " +
        s"${STORAGE_DECOMMISSION_ENABLED.key} and " +
        s"${STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key} and " +
        s"${SPARK_SHUFFLE_EXTERNAL_STORAGE_ENABLED.key} are true.")
    }
    if (!conf.get(SHUFFLE_MANAGER).equals(SHUFFLE_MANAGER.defaultValueString)) {
      throw new SparkException(
        s"Custom shuffle manager, ${conf.get(SHUFFLE_MANAGER)}, is not supported")
    }
    if (conf.get(SPECULATION_ENABLED)) {
      throw new SparkException(s"${SPECULATION_ENABLED.key} should be disabled to use " +
        s"${SPARK_SHUFFLE_EXTERNAL_STORAGE_ENABLED.key}")
    }
  }

  /** Test helper */
  private[spark] def enableExternalShuffleStorage(conf: SparkConf): SparkConf = {
    conf
      .set(DYN_ALLOCATION_ENABLED, true)
      .set(DECOMMISSION_ENABLED, true)
      .set(STORAGE_DECOMMISSION_ENABLED, true)
      .set(STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED, true)
      .set(SPARK_SHUFFLE_EXTERNAL_STORAGE_ENABLED, true)
  }

  def initialize(appId: String, conf: SparkConf): Unit = {
    if (isDisabled(conf)) {
      shutdown()
      return
    }
    val key = appId + conf.get(SPARK_SHUFFLE_EXTERNAL_STORAGE_SEPARATOR) + "_start"
    conf.set("spark.app.id", appId)
    val file = File.createTempFile("spark", ".tmp")
    putObject(conf, key, file)
    file.delete()
  }

  /** Register the external shuffle block manager and its RPC endpoint. */
  def registerBlockManager(master: BlockManagerMaster, conf: SparkConf): Unit = withCheck(conf) {
    master.registerBlockManager(
      EXTERNAL_BLOCK_MANAGER_ID,
      Array.empty[String], 0, 0, new ExternalShuffleStorageRpcEndpointRef(conf))
  }

  /** Report block status to block manager master and map output tracker master. */
  def reportBlockStatus(
      blockManager: BlockManager,
      shuffleId: Int,
      mapId: Long,
      dataLength: Long): Boolean = {
    val master = blockManager.master
    if (master == null) {
      logDebug("blockManager.master is null")
    }
    val blockId = ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID)
    logDebug(s"Report block status $blockId")
    val ret = master.updateBlockInfo(
      EXTERNAL_BLOCK_MANAGER_ID, blockId, StorageLevel.DISK_ONLY, memSize = 0, dataLength)
    if (ret && master.getLocations(blockId).isEmpty) {
      logError(s"Cannot find updated blocks: ${master.getLocations(blockId)}")
    }
    ret
  }

  /** Clean up the external storage location per application */
  def cleanUpAll(conf: SparkConf): Unit = {
    cleanUp(conf, getAppIdPrefix(conf))
  }

  /** Clean up all index and data files for the given shuffleId */
  def cleanUpShuffle(conf: SparkConf, shuffleId: Int): Unit = {
    cleanUp(conf, getAppIdPrefix(conf) + s"shuffle_${shuffleId}_")
  }

  /** Clean up an index and a data file for the given shuffle information */
  def cleanUpShuffleBlock(conf: SparkConf, shuffleId: Int, mapId: Long, reduceId: Int): Unit = {
    cleanUp(conf, getAppIdPrefix(conf) + s"shuffle_${shuffleId}_${mapId}_$reduceId")
  }

  /** Upload to external storage synchronously. */
  def upload(conf: SparkConf, bm: BlockManager, shuffleBlockInfo: ShuffleBlockInfo)
    : Boolean = withCheckOrFalse(conf) {
    setFileSystemIfNeeded(conf)
    val shuffleId = shuffleBlockInfo.shuffleId
    val mapId = shuffleBlockInfo.mapId
    val (indexFile, dataFile) = bm.migratableResolver.getMigrationFiles(shuffleBlockInfo)

    val parent = getAppIdPrefix(conf)
    logInfo(s"Uploading ${indexFile.getAbsolutePath}")
    putObject(conf, parent + indexFile.getName, indexFile)
    logInfo(s"Uploading ${dataFile.getAbsolutePath}")
    putObject(conf, parent + dataFile.getName, dataFile)

    if (doesObjectExist(conf, parent + indexFile.getName) &&
        doesObjectExist(conf, parent + dataFile.getName)) {
      logInfo(s"Uploading ${dataFile.getAbsolutePath}")
      reportBlockStatus(bm, shuffleId, mapId, dataFile.length)
    } else {
      logInfo(s"Still invisible ${parent + dataFile.getName}")
      false
    }
  }

  /** Delete the corresponding file from external storage asynchronously. */
  def delete(conf: SparkConf, file: File): Unit = withCheck(conf) {
    val parent = getAppIdPrefix(conf)
    val key = parent + file.getName
    deleteExecutor.execute(() => deleteObject(conf, key))
  }

  /** Try to read local file first, then go for the external storage. */
  def read(resolver: ShuffleBlockResolver, blockId: BlockId): ManagedBuffer = {
    // In Spark 3.0, IndexShuffleBlockResolver is the only implementation for ShuffleBlockResolver
    assert(resolver.isInstanceOf[IndexShuffleBlockResolver])
    assert(blockId.isShuffle)

    val indexShuffleBlockResolver = resolver.asInstanceOf[IndexShuffleBlockResolver]
    val isLocal = blockId match {
      case ShuffleBlockId(shuffleId, mapId, _) =>
        indexShuffleBlockResolver.getDataFile(shuffleId, mapId, None).exists()
      case ShuffleBlockBatchId(shuffleId, mapId, _, _) =>
        indexShuffleBlockResolver.getDataFile(shuffleId, mapId, None).exists()
      case _ => false
    }
    if (isLocal) {
      indexShuffleBlockResolver.getBlockData(blockId)
    } else {
      ExternalShuffleStorage.read(SparkEnv.get.conf, blockId)
    }
  }

  /**
   * Read a ManagedBuffer from external storage synchronously.
   * If we have an enough space on the local disk, we may download the file later.
   */
  def read(conf: SparkConf, blockId: BlockId): ManagedBuffer = withCheckOrException(conf) {
    logDebug(s"Read $blockId")

    val (shuffleId, mapId, startReduceId, endReduceId) = blockId match {
      case id: ShuffleBlockId =>
        (id.shuffleId, id.mapId, id.reduceId, id.reduceId + 1)
      case batchId: ShuffleBlockBatchId =>
        (batchId.shuffleId, batchId.mapId, batchId.startReduceId, batchId.endReduceId)
      case _ =>
        throw new IllegalArgumentException("unexpected shuffle block id format: " + blockId)
    }

    val parent = getAppIdPrefix(conf)
    val indexFile = parent + ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
    val start = startReduceId * 8L
    val end = endReduceId * 8L
    // If we download the file, we can use FileSegmentManagedBuffer.
    Utils.tryWithResource(getObject(conf, indexFile, start, end + 8L)) { inputStream =>
      Utils.tryWithResource(new DataInputStream(inputStream)) { index =>
        val offset = index.readLong()
        index.skip(end - (start + 8L))
        val nextOffset = index.readLong()
        val dataFile = parent + ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
        val data = getObject(conf, dataFile, offset, nextOffset - 1)
        val size = nextOffset - 1 - offset
        logDebug(s"To byte array $size")
        val startTimeNs = System.nanoTime()
        val array = toByteArray(data, size)
        logDebug(s"Took ${(System.nanoTime() - startTimeNs) / (1000 * 1000)}ms")
        data.close()
        new NioManagedBuffer(java.nio.ByteBuffer.wrap(array))
      }
    }
  }

  /**
   * `buf.createInputStream` is supposed to succeed if Spark driver has all latest
   * information.
   */
  def createInputStream(buf: ManagedBuffer, blockId: BlockId): InputStream = {
    try {
      buf.createInputStream()
    } catch {
      // For shuffle blocks, retry to external shuffle storage
      case ioe: IOException if blockId.isInternalShuffle =>
        try {
          logWarning(s"Fallback to read $blockId from external shuffle storage")
          ExternalShuffleStorage.read(SparkEnv.get.conf, blockId).createInputStream()
        } catch {
          case e: IOException =>
            logDebug(e.getMessage)
            // We don't throw the exceptions from the fallback logic.
            // Instead, propagate the original exception.
            throw ioe
        }
      case e: Throwable => throw e
    }
  }
}
