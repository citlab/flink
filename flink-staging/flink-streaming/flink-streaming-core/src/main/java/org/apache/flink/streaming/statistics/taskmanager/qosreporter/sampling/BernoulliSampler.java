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

/**
 * Combines a bernoulli sampling design ({@link #shouldTakeSamplePoint()} returns true
 * with definable probability) with sample mean and variance computation
 * according to Knuth.
 *
 * It implements the version described in the following wikipedia article:
 * http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#
 * Incremental_algorithm
 *
 * Quote: "This algorithm is much less prone to loss of precision due to massive
 * cancellation, but might not be as efficient because of the division operation
 * inside the loop."
 *
 * @author Bjoern Lohrmann
 *
 */
public class BernoulliSampler {

	private final BernoulliSampleDesign samplingDesign;

	private int noSamplePoints;
	private double mean;
	private double s;
	private long samplingBeginTime;

	public BernoulliSampler(double samplingProbability) {
		samplingDesign = new BernoulliSampleDesign(samplingProbability);
		reset(System.currentTimeMillis());
	}

	public boolean shouldTakeSamplePoint() {
		return samplingDesign.shouldSample();
	}

	public void addSamplePoint(double x) {
		noSamplePoints++;

		if (noSamplePoints == 1) {
			mean = x;
			s = 0;
		} else {
			double delta = (x - mean);
			mean += delta / noSamplePoints;
			s += delta * (x - mean);
		}
	}

	public boolean hasSample() {
		return noSamplePoints > 1;
	}

	public double getMean() {
		return mean;
	}

	public double getVariance() {
		return s / (noSamplePoints - 1);
	}

	public Sample drawSampleAndReset(long now) {
		Sample sample = new Sample((int) (now - samplingBeginTime),
				noSamplePoints, getMean(), getVariance());
		reset(now);
		return sample;
	}

	public void reset(long now) {
		samplingBeginTime = now;
		noSamplePoints = 0;
		mean = 0;
		s = 0;
		samplingDesign.reset();
	}
}
