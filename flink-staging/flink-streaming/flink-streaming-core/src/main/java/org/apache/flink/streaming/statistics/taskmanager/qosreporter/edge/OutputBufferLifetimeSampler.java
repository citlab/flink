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

package org.apache.flink.streaming.statistics.taskmanager.qosreporter.edge;

import org.apache.flink.streaming.statistics.taskmanager.qosreporter.sampling.BernoulliSampleDesign;

public class OutputBufferLifetimeSampler {

	private final BernoulliSampleDesign sampleDesign;
	private int samples;
	private long outputBufferLifetimeSampleBeginTime;
	private long outputBufferLifetimeSampleSum;


	public OutputBufferLifetimeSampler(double samplingProbability) {
		sampleDesign = new BernoulliSampleDesign(samplingProbability);
		outputBufferLifetimeSampleBeginTime = -1;
		reset();
	}

	public void reset() {
		samples = 0;
		outputBufferLifetimeSampleSum = 0;
		sampleDesign.reset();
	}

	public void outputBufferSent() {
		if (outputBufferLifetimeSampleBeginTime != -1) {
			outputBufferLifetimeSampleSum += System.nanoTime() - outputBufferLifetimeSampleBeginTime;
			outputBufferLifetimeSampleBeginTime = -1;
			samples++;
		}
	}

	public void outputBufferAllocated() {
		if (sampleDesign.shouldSample()) {
			outputBufferLifetimeSampleBeginTime = System.nanoTime();
		}
	}

	public boolean hasSample() {
		return samples > 0;
	}


	public double getMeanOutputBufferLifetimeMillis() {
		return outputBufferLifetimeSampleSum / (1000000.0 * samples);
	}
}
