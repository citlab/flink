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
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.QosStatisticsForwarder;

import java.util.HashMap;

/**
 * Manages forwarder instances on a job basis.
 */
public class QosStatisticsForwarderFactory {
	private final static HashMap<JobID, QosStatisticsForwarder> runningInstances
			= new HashMap<JobID, QosStatisticsForwarder>();

	public static QosStatisticsForwarder getOrCreateForwarder(Environment env) {
		JobID jobID = env.getJobID();
		QosStatisticsForwarder forwarder;

		synchronized (runningInstances) {
			forwarder = runningInstances.get(jobID);

			if (forwarder == null) {
				forwarder = new QosStatisticsForwarder(jobID, env.getJobManager(), env.getJobConfiguration());
				runningInstances.put(jobID, forwarder);
			}
		}

		return forwarder;
	}

	public static void removeForwarderInstance(JobID jobID) {
		synchronized (runningInstances) {
			runningInstances.remove(jobID);
		}
	}
}
