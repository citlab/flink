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
 * Implements the Kraemer-Langenbach-Belz formula for G/G/1 queueing systems, which
 * is based on the "standard" Kingman formula.
 * 
 * @author Bjoern Lohrmann
 */
public class GG1ServerKLB extends GG1ServerKingman {

	public GG1ServerKLB(JobVertexID groupVertexID, int minSubtasks,
			int maxSubtasks, QosGroupEdgeSummary edgeSummary) {
		super(groupVertexID, minSubtasks, maxSubtasks, edgeSummary);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected double getQueueWaitUnfitted(int newP, double rho) {
		return super.getQueueWaitUnfitted(newP, rho) * getKLBCorrectionFactor(rho);
	}

	private double getKLBCorrectionFactor(double rho) {
		double exponent;

		if (cA > 1) {
			exponent = (-1) * (1 - rho) * (cA * cA - 1)
					/ (cA * cA + 4 * cS * cS);
		} else {
			exponent = (-2 * (1 - rho) * (1 - cA * cA) * (1 - cA * cA))
					/ (3 * rho * (cA * cA + cS * cS));
		}

		return Math.exp(exponent);
	}
}
