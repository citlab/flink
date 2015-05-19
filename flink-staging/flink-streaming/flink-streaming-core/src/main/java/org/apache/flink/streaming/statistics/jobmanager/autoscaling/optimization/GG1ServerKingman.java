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

package org.apache.flink.streaming.statistics.jobmanager.autoscaling.optimization;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.statistics.taskmanager.qosmanager.QosGroupEdgeSummary;

/**
 * Implements the "standard" Kingman formula for G/G/1 queueing systems, which
 * is an approximation of queue waiting time.
 * 
 * @author Bjoern Lohrmann
 */
public class GG1ServerKingman extends GG1Server {
	
	public GG1ServerKingman(JobVertexID groupVertexID, int minSubtasks,
			int maxSubtasks, QosGroupEdgeSummary edgeSummary) {
		super(groupVertexID, minSubtasks, maxSubtasks, edgeSummary);
	}

	protected double getQueueWaitUnfitted(int newP, double rho) {
		double left = rho * S / (1 - rho);
		double right = ((cA * cA) + (cS * cS)) / 2;

		return left * right;
	}
}
