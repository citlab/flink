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

package org.apache.flink.streaming.statistics.taskmanager.qosreporter.sampling;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class Sample implements IOReadableWritable {

	private int samplingDurationMillis;
	private int noOfSamplePoints;
	private double mean;
	private double variance;

	public Sample() {
	}

	public Sample(int samplingDurationMillis, int noOfSamplePoints,
			double mean, double variance) {

		this.samplingDurationMillis = samplingDurationMillis;
		this.noOfSamplePoints = noOfSamplePoints;
		this.mean = mean;
		this.variance = variance;
	}

	public int getSamplingDurationMillis() {
		return samplingDurationMillis;
	}

	public double getMean() {
		return mean;
	}

	public double getVariance() {
		return variance;
	}

	public int getNoOfSamplePoints() {
		return noOfSamplePoints;
	}

	public Sample fuseWithDisjunctSample(Sample other) {

		int newSamplingDuration = samplingDurationMillis
				+ other.samplingDurationMillis;

		int newSamplePoints = noOfSamplePoints + other.noOfSamplePoints;

		double newMean = mean * (((double) noOfSamplePoints) / newSamplePoints);
		newMean += other.mean
				* (((double) other.noOfSamplePoints) / newSamplePoints);

		double ess = (noOfSamplePoints - 1) * variance
				+ (other.noOfSamplePoints - 1) * other.variance;
		double tgss = noOfSamplePoints * (mean - newMean) * (mean - newMean)
				+ other.noOfSamplePoints * (other.mean - newMean)
				* (other.mean - newMean);
		double newVariance = (ess / newSamplePoints) + (tgss / newSamplePoints);

		return new Sample(newSamplingDuration, newSamplePoints, newMean,
				newVariance);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeInt(samplingDurationMillis);
		out.writeInt(noOfSamplePoints);
		out.writeDouble(mean);
		out.writeDouble(variance);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		samplingDurationMillis = in.readInt();
		noOfSamplePoints = in.readInt();
		mean = in.readDouble();
		variance = in.readDouble();
	}

	/**
	 * Rescales the mean and variance value. Produces a sample with new mean
	 * this.mean*factor and new variance this.variance* factor * factor.
	 *
	 */
	public Sample rescale(double factor) {
		return new Sample(samplingDurationMillis, noOfSamplePoints, mean
				* factor, variance * factor * factor);
	}
}
