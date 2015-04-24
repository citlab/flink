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

package org.apache.flink.streaming.statistics.taskmanager.qosmodel;

/**
 * A Qos value represents a set of measurements ("samples") of some runtime
 * aspect, e.g. a set of vertex latency measurements. A Qos value holds a timestamp, a mean
 * and optionally the variance of a set of set of measurements.
 *
 * @author Bjoern Lohrmann
 *
 */
public class QosValue {

	private final long timestamp;

	private final double mean;

	private final double variance;

	private int weight;

	public QosValue(double mean, long timestamp) {
		this(mean, 1, timestamp);
	}

	public QosValue(double mean, int weight, long timestamp) {
		this.mean = mean;
		this.variance = -1;
		this.weight = weight;
		this.timestamp = timestamp;
	}

	public QosValue(double mean, double variance, int weight, long timestamp) {
		this.mean = mean;
		this.variance = variance;
		this.weight = weight;
		this.timestamp = timestamp;
	}

	public long getTimestamp() {
		return this.timestamp;
	}

	public double getMean() {
		return this.mean;
	}

	public double getVariance() {
		return this.variance;
	}

	public boolean hasVariance() {
		return this.variance != -1;
	}

	public int getWeight() {
		return weight;
	}
}
