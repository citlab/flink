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

package org.apache.flink.streaming.statistics.taskmanager.qosreporter.vertex;

import org.apache.flink.streaming.statistics.taskmanager.qosreporter.sampling.BernoulliSampler;
import org.apache.flink.streaming.statistics.taskmanager.qosreporter.sampling.Sample;

/**
 * Samples the elapsed time between a read on a specific input gate identified
 * and the beginning of the next attempt read on any other input gate. Elapsed time is sampled in
 * microseconds.
 */
public class InputGateInterReadTimeSampler {
	
	/**
	 * Samples the elapsed time between a read on the input gate identified
	 * {@link #inputGateIndex} and the next read on any other input gate.
	 * Elapsed time is sampled in microseconds.
	 */
	private final BernoulliSampler readReadTimeSampler;

	private long lastSampleReadTime;

	public InputGateInterReadTimeSampler(double samplingProbability) {
		readReadTimeSampler = new BernoulliSampler(samplingProbability);
		lastSampleReadTime = -1;
	}
	
	public void recordReceivedOnIg() {
		if (readReadTimeSampler.shouldTakeSamplePoint()) {
			lastSampleReadTime = System.nanoTime();
		}
	}
	
	public void tryingToReadRecordFromAnyIg() {
		if (lastSampleReadTime != -1) {
			// if lastSampleReadTime is set then we should sample
			readReadTimeSampler.addSamplePoint((System.nanoTime() - lastSampleReadTime) / 1000.0);
			lastSampleReadTime = -1;
		}
	}
	
	public boolean hasSample() {
		return readReadTimeSampler.hasSample();
	}

	public Sample drawSampleAndReset(long now) {
		return readReadTimeSampler.drawSampleAndReset(now);
	}
}
