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

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.SparkConf
import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef, RpcTimeout}
import org.apache.spark.storage._
import org.apache.spark.storage.BlockManagerMessages._

/**
 * Since ExternalShuffleStorageBlockManager exists as a name, not a service,
 * we need to handle the logic from the caller side.
 */
class ExternalShuffleStorageRpcEndpointRef(conf: SparkConf) extends RpcEndpointRef(conf) {
  import scala.concurrent.ExecutionContext.Implicits.global

  override def address: RpcAddress = null

  override def name: String = "external"

  override def send(message: Any): Unit = {
    logInfo(s"Send $message to $name")
  }

  override def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T] = {
    // Monitoring `ToBlockManagerSlave` messages
    message match {
      case RemoveBlock(ShuffleBlockId(shuffleId, mapId, reduceId)) =>
        logDebug(s"Clean up $message")
        ExternalShuffleStorage.cleanUpShuffleBlock(conf, shuffleId, mapId, reduceId)

      case _: RemoveBlock =>
        logDebug(s"$message is ignored")

      case _: ReplicateBlock => // no-op
        logDebug(s"$message is ignored")

      case RemoveShuffle(shuffleId) =>
        logDebug(s"We need to remove all shuffle data for $shuffleId")
        ExternalShuffleStorage.cleanUpShuffle(conf, shuffleId)

      case _: RemoveRdd => // no-op
        logDebug(s"$message is ignored")

      case _: RemoveBroadcast => // no-op
        logDebug(s"$message is ignored")

      case TriggerThreadDump => // no-op
        logDebug(s"$message is ignored")

      case _ => // unknown message types in
        logDebug(s"$message is ignored")
    }
    Future{true.asInstanceOf[T]}
  }
}
