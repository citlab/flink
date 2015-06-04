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
import org.apache.flink.streaming.statistics.util.QosStatisticsConfig;

public abstract class GG1Server {

	public static final double MAX_UTILIZATION = 0.9;

	private final JobVertexID groupVertexID;

	protected final double lambdaTotal;

	protected final int p;

	protected final double cA;

	protected final double S;

	protected final double cS;

	protected final double fittingFactor;

	protected final int lowerBoundParallelism;

	protected final int upperBoundParallelism;

	public GG1Server(JobVertexID groupVertexID, int minSubtasks,
			int maxSubtasks, QosGroupEdgeSummary edgeSummary) {
		this.groupVertexID = groupVertexID;

		// measured values
		lambdaTotal = edgeSummary.getMeanEmissionRate()
				* edgeSummary.getActiveEmitterVertices();
		p = edgeSummary.getActiveConsumerVertices();
		S = edgeSummary.getMeanConsumerVertexLatency() / 1000;
		cS = edgeSummary.getMeanConsumerVertexLatencyCV();
		cA = edgeSummary.getMeanConsumerVertexInterarrivalTimeCV();
		
		double theoreticalFittingFactor = (edgeSummary.getTransportLatencyMean() / 1000)
				/ getQueueWaitUnfitted(p);
		if(theoreticalFittingFactor < QosStatisticsConfig.getElasticScalingMinFittingFactor()) {
			fittingFactor = QosStatisticsConfig.getElasticScalingMinFittingFactor();
		} else if (theoreticalFittingFactor > QosStatisticsConfig.getElasticScalingMaxFittingFactor()) {
			fittingFactor = QosStatisticsConfig.getElasticScalingMaxFittingFactor();
		} else {
			fittingFactor = theoreticalFittingFactor;
		}

		int pWithMaxAllowedUtil = (int) Math.ceil(lambdaTotal * S / MAX_UTILIZATION);
		lowerBoundParallelism = Math.min(Math.max(pWithMaxAllowedUtil, minSubtasks), maxSubtasks);
		upperBoundParallelism = maxSubtasks;
	}

	public JobVertexID getGroupVertexID() {
		return groupVertexID;
	}

	protected abstract double getQueueWaitUnfitted(int newP, double rho);
	
	private double getQueueWaitUnfitted(int newP) {
		double rho = getMeanUtilization(newP);

		if (rho < 1) {
			return getQueueWaitUnfitted(newP, rho);
		} else {
			return Double.POSITIVE_INFINITY;
		}
	}
	
	public double getQueueWait(int newP) {
		return getQueueWaitUnfitted(newP) * fittingFactor;
	}

	public int getLowerBoundParallelism() {
		return lowerBoundParallelism;
	}
		
	public double computeQueueWaitGradient(int newP) {
		if (newP >= getUpperBoundParallelism()) {
			return Double.POSITIVE_INFINITY;
		} else if (newP < getLowerBoundParallelism()) {
			return Double.NEGATIVE_INFINITY;
		} else {
			return getQueueWait(newP + 1) - getQueueWait(newP);
		}
	}

	public int getUpperBoundParallelism() {
		return upperBoundParallelism;
	}
	
	public int getCurrentParallelism() {
		return p;
	}
	
	public double getMeanUtilization(int newP) {
		return S * lambdaTotal / newP;
	}
	
	public double getCurrentMeanUtilization() {
		return getMeanUtilization(p);
	}
	
	/**
	 * Returns a degree of parallelism so that its queue wait gradient
	 * {@link #computeQueueWaitGradient(int)} is greater or equal to the given
	 * threshold.
	 * 
	 * @param gradientThreshold
	 *            A negative floating point value indication the minimum desired
	 *            queue wait gradient.
	 * 
	 * @return A value between {@link #lowerBoundParallelism} and
	 *         {@link #upperBoundParallelism} (both inclusive).
	 * 
	 */
	public int computeParallelismForGradientThreshold(double gradientThreshold) {		
		double a = fittingFactor * lambdaTotal * S * S * (cA * cA + cS * cS) / 2;
		double b = lambdaTotal * S;
		double c = gradientThreshold;

		// we are looking for a new parallelism x so that the queueWaitGradient
		// is >= c, or in other words, we are looking for an x s.t.
		// a/(x+1-b) - a/(x-b) >= c
		//
		// these are the p,q from pq-formula for solving the quadratic equation
		// a/(x+1-b) - a/(x-b) == c
		double p = 1 - 2 * b;
		double q = (a + c * (b * b - b)) / c;

		// since the pq formula gives us two solutions for x, we always take the larger 
		// one (the smaller one is invalid)
		double x = -(p / 2) + Math.sqrt(p*p - 4*q)/2;

		if (Double.isNaN(x)) {
			throw new RuntimeException(
					"Could not compute parallelism for a queue wait gradient threshold. This is a bug.");
		}
		
		int ret = (int) Math.min(upperBoundParallelism,
				Math.max(lowerBoundParallelism, Math.floor(x + 1)));
		
		int upDiff = 0;
		while (computeQueueWaitGradient(ret + upDiff) < gradientThreshold) {
			upDiff++;
		}

		int downDiff = 0;
		while (computeQueueWaitGradient(ret - downDiff - 1) >= gradientThreshold) {
			downDiff++;
		}
		
		return ret + upDiff - downDiff;
	}
	
	/**
	 * Returns a degree of parallelism so that the queue wait time
	 * {@link #getQueueWait(int)} is lower or equal to the given
	 * threshold. If no valid value can be found, the upper bound on parallelism
	 * is returned.
	 * 
	 * @param queueWaitThreshold Queueing wait threshold for returned parallelism.
	 * @return A value from between {@link #lowerBoundParallelism} and
	 *         {@link #upperBoundParallelism} (both inclusive).
	 * 
	 */
	public int computeParallelismForQueueWaitThreshold(double queueWaitThreshold) {
		// determine feasibility
		if (getQueueWait(upperBoundParallelism) > queueWaitThreshold) {
			return upperBoundParallelism;
		}
		
		double a = fittingFactor * lambdaTotal * S * S * (cA * cA + cS * cS) / 2;
		double b = lambdaTotal * S;
		
		// we are looking for a parallelism x so that the queue wait time is
		// <= w, or in other words, we for a parallelism x so that
		// a/(x-b) <= queueWaitThreshold
		
		double x = (a / queueWaitThreshold) + b;
		
		return (int) Math.min(upperBoundParallelism,
				Math.max(lowerBoundParallelism, Math.floor(x + 1)));
	}
	
	public boolean isElastic() {
		return lowerBoundParallelism != upperBoundParallelism;
	}
}
