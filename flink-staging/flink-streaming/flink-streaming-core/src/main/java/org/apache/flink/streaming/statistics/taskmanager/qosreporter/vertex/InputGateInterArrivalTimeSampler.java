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
import org.apache.flink.streaming.statistics.util.StreamUtil;

public class InputGateInterArrivalTimeSampler {
	
	/**
	 * Samples records interarrival times in microseconds. These are computed
	 * from buffer interarrival times given in nanoseconds.
	 */
	private final BernoulliSampler interarrivalTimeSampler;
	
	private Long[] accBufferInterarrivalTimes = new Long[0];

	public InputGateInterArrivalTimeSampler(double samplingProbability) {
		this.interarrivalTimeSampler = new BernoulliSampler(samplingProbability);
	}

	public void inputBufferConsumed(int channelIndex,
			long bufferInterarrivalTimeNanos, int recordsReadFromBuffer) {
		
		if (accBufferInterarrivalTimes.length <= channelIndex) {
			accBufferInterarrivalTimes = StreamUtil.setInArrayAt(
					accBufferInterarrivalTimes, Long.class, channelIndex, null);
		}

		Long channelAccumulatedTime = accBufferInterarrivalTimes[channelIndex];

		if (channelAccumulatedTime == null && recordsReadFromBuffer > 0) {
			startSampleIfNecessary(channelIndex);

		} else if (channelAccumulatedTime != null) {
			finalizeOrAccumulateSamplePoint(channelIndex,
					bufferInterarrivalTimeNanos, recordsReadFromBuffer);
		}
	}

	private void finalizeOrAccumulateSamplePoint(int channelIndex,
			long interarrivalTimeNanos, int recordsReadFromBuffer) {

		if (recordsReadFromBuffer > 0) {
			finalizeSamplePoint(channelIndex, interarrivalTimeNanos,
					recordsReadFromBuffer);
			startSampleIfNecessary(channelIndex);
		} else {
			accBufferInterarrivalTimes[channelIndex] += interarrivalTimeNanos;
		}
	}

	private void finalizeSamplePoint(int channelIndex,
			long interarrivalTimeNanos, int recordsReadFromBuffer) {
		
		Long channelAccumulatedTime = accBufferInterarrivalTimes[channelIndex];
		interarrivalTimeSampler.addSamplePoint((channelAccumulatedTime
				+ interarrivalTimeNanos) / 1000.0);
		for (int i = 0; i < recordsReadFromBuffer - 1; i++) {
			interarrivalTimeSampler.addSamplePoint(0);
		}
		accBufferInterarrivalTimes[channelIndex] = null;
	}

	private void startSampleIfNecessary(int channelIndex) {
		if (interarrivalTimeSampler.shouldTakeSamplePoint()) {
			accBufferInterarrivalTimes[channelIndex] = Long.valueOf(0);
		}
	}
	
	public boolean hasSample() {
		return interarrivalTimeSampler.hasSample();
	}
	
	public Sample drawSampleAndReset(long now) {
		return interarrivalTimeSampler.drawSampleAndReset(now);
	}
}
