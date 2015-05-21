/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.statistics

import akka.actor._
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.messages.ExecutionGraphMessages.{ExecutionStateChanged, JobStatusChanged}

/**
 * The CentralStatisticsActor handles statistics on a central (job manager) point.
 *
 * @author Sascha Wolke
 */
class CentralStatisticsActor(val executionGraph: ExecutionGraph,
                             val handler: AbstractCentralStatisticsHandler)
      extends Actor with ActorLogMessages with ActorLogging {

  override def preStart(): Unit = {
    handler.open(executionGraph.getJobID, executionGraph)
  }

  override def receiveWithLogMessages: Receive = {
    case StatisticReport(jobID, statistic) =>
      handler.handleStatistic(statistic)

    case executionState@ExecutionStateChanged(_, _, _, _, _, _, _, _, _) =>
      handler.handleExecutionStateChanged(executionState)

    case jobStatus@JobStatusChanged(_, newState, _, _) =>
      handler.handleJobStatusChanged(jobStatus)

      newState match {
        case JobStatus.FINISHED | JobStatus.CANCELED | JobStatus.FAILED =>
          context.stop(self)
        case _ =>
      }
  }

  override def postStop(): Unit = {
    handler.close
  }
}

object CentralStatisticsActor {
  def spawn(context: ActorContext, executionGraph: ExecutionGraph,
            handler: AbstractCentralStatisticsHandler): ActorRef = {

    val ref = context.system.actorOf(Props(new CentralStatisticsActor(executionGraph, handler)))
    executionGraph.registerJobStatusListener(ref)
    executionGraph.registerExecutionListener(ref)
    ref
  }
}

// statistic report from task manager
case class StatisticReport(jobID: JobID, content: CustomStatistic)
// commit by local scheduler every report interval
case class ReportStatistics()

// override this class with your custom statistics:
abstract class CustomStatistic extends Serializable
