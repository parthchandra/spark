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

package org.apache.spark.ui.exec

import javax.servlet.http.HttpServletRequest

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.xml.{Node, Unparsed}
import org.apache.spark.status.api.v1.ExecutorSummary
import org.apache.spark.ui.{UIUtils, WebUIPage}

// This isn't even used anymore -- but we need to keep it b/c of a MiMa false positive
private[ui] case class ExecutorSummaryInfo(
    id: String,
    hostPort: String,
    rddBlocks: Int,
    memoryUsed: Long,
    diskUsed: Long,
    activeTasks: Int,
    failedTasks: Int,
    completedTasks: Int,
    totalTasks: Int,
    totalDuration: Long,
    totalInputBytes: Long,
    totalShuffleRead: Long,
    totalShuffleWrite: Long,
    maxMemory: Long,
    executorLogs: Map[String, String])

private[ui] class ExecutorsPage(
    parent: ExecutorsTab,
    threadDumpEnabled: Boolean,
    ajaxEnabled: Boolean)
  extends WebUIPage("") {
  private val listener = parent.listener

  def executorList(): Seq[ExecutorSummary] = {
    val listener = parent.listener
    listener.synchronized {
      // The follow codes should be protected by `listener` to make sure no executors will be
      // removed before we query their status. See SPARK-12784.
      (0 until listener.activeStorageStatusList.size).map { statusId =>
        ExecutorsPage.getExecInfo(listener, statusId, isActive = true)
      } ++ (0 until listener.deadStorageStatusList.size).map { statusId =>
        ExecutorsPage.getExecInfo(listener, statusId, isActive = false)
      }
    }
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    def allExecutorsDataScript: Seq[Node] = {
      <script>
        {Unparsed {
        "var allExecutorsData='" + ExecutorsPage.mapper.writeValueAsString(executorList) + "';"
      }}
      </script>
    }
    val content =
      <div>
        {
          <div id="active-executors"></div> ++
          <script src={UIUtils.prependBaseUri("/static/utils.js")}></script> ++
          <script src={UIUtils.prependBaseUri("/static/executorspage-template.js")}></script> ++
          <script src={UIUtils.prependBaseUri("/static/executorspage.js")}></script> ++
          {if (!ajaxEnabled) allExecutorsDataScript else Seq.empty} ++
          <script>setThreadDumpEnabled({threadDumpEnabled})</script> ++
          <script>setAjaxEnabled({ajaxEnabled})</script>
        }
      </div>;

    UIUtils.headerSparkPage("Executors", content, parent, useDataTables = true)
  }
}

private[spark] object ExecutorsPage {

  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /** Represent an executor's info as a map given a storage status index */
  def getExecInfo(
      listener: ExecutorsListener,
      statusId: Int,
      isActive: Boolean): ExecutorSummary = {
    val status = if (isActive) {
      listener.activeStorageStatusList(statusId)
    } else {
      listener.deadStorageStatusList(statusId)
    }
    val execId = status.blockManagerId.executorId
    val hostPort = status.blockManagerId.hostPort
    val rddBlocks = status.numBlocks
    val memUsed = status.memUsed
    val maxMem = status.maxMem
    val diskUsed = status.diskUsed
    val taskSummary = listener.executorToTaskSummary.getOrElse(execId, ExecutorTaskSummary(execId))

    new ExecutorSummary(
      execId,
      hostPort,
      isActive,
      rddBlocks,
      memUsed,
      diskUsed,
      taskSummary.totalCores,
      taskSummary.tasksMax,
      taskSummary.tasksActive,
      taskSummary.tasksFailed,
      taskSummary.tasksComplete,
      taskSummary.tasksActive + taskSummary.tasksFailed + taskSummary.tasksComplete,
      taskSummary.duration,
      taskSummary.jvmGCTime,
      taskSummary.inputBytes,
      taskSummary.shuffleRead,
      taskSummary.shuffleWrite,
      maxMem,
      taskSummary.executorLogs
    )
  }
}
