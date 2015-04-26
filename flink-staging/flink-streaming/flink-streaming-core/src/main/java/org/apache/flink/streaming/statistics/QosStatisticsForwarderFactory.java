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

package org.apache.flink.streaming.statistics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.QosReportForwarderThread;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.StreamTaskQosCoordinator;

import java.util.HashMap;

/**
 * Manages forwarder instances on a job basis.
 */
public class QosStatisticsForwarderFactory {
	private final static HashMap<JobID, QosReportForwarderThread> runningInstances
			= new HashMap<JobID, QosReportForwarderThread>();

	public static QosReportForwarderThread getOrCreateForwarder(
			StreamTaskQosCoordinator coordinator, Environment env) {

		JobID jobID = env.getJobID();
		QosReportForwarderThread forwarder;

		synchronized (runningInstances) {
			forwarder = runningInstances.get(jobID);

			if (forwarder == null) {
				forwarder = new QosReportForwarderThread(coordinator, env);
				runningInstances.put(jobID, forwarder);
			}
		}

		return forwarder;
	}

	public static void removeForwarderInstance(JobID jobID) {
		QosReportForwarderThread forwarder = runningInstances.get(jobID);

		if (forwarder != null) {
			forwarder.shutdown();

			synchronized (runningInstances) {
				runningInstances.remove(jobID);
			}
		}
	}
}
