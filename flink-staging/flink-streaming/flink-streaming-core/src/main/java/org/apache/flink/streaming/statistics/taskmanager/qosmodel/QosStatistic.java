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

import java.util.LinkedList;

public class QosStatistic {

	private final LinkedList<QosValue> sortedByTs;

	private final int statisticWindowSize;

	private int noOfStoredValues;

	private int sumOfWeights;

	private double sumOfWeightedMeans;

	private double sumOfWeightedVariances;

	private final boolean hasVariance;

	private QosValue statisticCache;

	public QosStatistic(int statisticWindowSize) {
		this(statisticWindowSize, false);
	}

	public QosStatistic(int statisticWindowSize, boolean hasVariance) {
		this.sortedByTs = new LinkedList<QosValue>();
		this.statisticWindowSize = statisticWindowSize;
		this.hasVariance = hasVariance;
		clear();
	}

	public void clear() {
		this.noOfStoredValues = 0;
		this.sumOfWeightedMeans = 0;
		this.sumOfWeightedVariances = 0;
		this.sumOfWeights = 0;
		this.sortedByTs.clear();
	}

	public void addValue(QosValue value) {
		if (value.hasVariance() != hasVariance) {
			throw new RuntimeException(
					"Cannot put QosValues without variance into a statistic with variance, or vice versa");
		}

		QosValue droppedValue = this.insertIntoSortedByTimestamp(value);

		if (droppedValue != null) {
			this.noOfStoredValues--;
			this.sumOfWeights -= droppedValue.getWeight();
			this.sumOfWeightedMeans -= droppedValue.getWeight() * droppedValue.getMean();

			if (hasVariance) {
				this.sumOfWeightedVariances -= (droppedValue.getWeight() -1) * droppedValue.getVariance();
			}
		}

		this.noOfStoredValues++;
		this.sumOfWeights += value.getWeight();
		this.sumOfWeightedMeans += value.getWeight() * value.getMean();

		if (hasVariance) {
			this.sumOfWeightedVariances += (value.getWeight() -1) * value.getVariance();
		}

		this.statisticCache = null;
	}

	private QosValue insertIntoSortedByTimestamp(QosValue value) {
		if (!this.sortedByTs.isEmpty()) {
			long lastTimestamp = this.sortedByTs.getLast().getTimestamp();

			if (lastTimestamp == value.getTimestamp()) {
				// Skip this (already added) entry. This might happen on vertices with
				// more than one input/output gate reporter combination.
				return null;

			} else if (lastTimestamp > value.getTimestamp()) {
				throw new IllegalArgumentException(
						"Trying to add stale Qos statistic values. This should not happen.");
			}
		}

		this.sortedByTs.add(value);

		if (this.noOfStoredValues >= this.statisticWindowSize) {
			return this.sortedByTs.removeFirst();
		}
		return null;
	}

	public QosValue getOldestValue() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot get the oldest value of empty value set");
		}
		return this.sortedByTs.getFirst();
	}

	public QosValue getNewestValue() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot get the newest value of empty value set");
		}
		return this.sortedByTs.getLast();
	}

	public double getMean() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot calculate the arithmetic mean of empty value set");
		}

		if (statisticCache == null) {
			refreshStatistic();
		}

		return statisticCache.getMean();
	}

	private void refreshStatistic() {

		double mean = sumOfWeightedMeans / sumOfWeights;

		if (hasVariance()) {
			double tgss = 0;
			for(QosValue value : sortedByTs) {
				double meanDiff = value.getMean() - mean;
				tgss += (meanDiff * meanDiff) * value.getWeight();
			}
			double variance = (sumOfWeightedVariances + tgss) / (sumOfWeights -1) ;

			statisticCache = new QosValue(mean, variance, sumOfWeights, getNewestValue().getTimestamp());
		} else {
			statisticCache = new QosValue(mean, sumOfWeights, getNewestValue().getTimestamp());
		}
	}

	public boolean hasVariance() {
		return this.hasVariance;
	}

	public double getVariance() {
		if (this.noOfStoredValues == 0) {
			throw new RuntimeException(
					"Cannot calculate the variance of empty value set");
		}

		if (statisticCache == null) {
			refreshStatistic();
		}

		return statisticCache.getVariance();
	}

	public boolean hasValues() {
		return this.noOfStoredValues > 0;
	}
}
