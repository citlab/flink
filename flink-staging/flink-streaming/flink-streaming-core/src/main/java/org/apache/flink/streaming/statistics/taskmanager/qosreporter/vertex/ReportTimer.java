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

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.flink.streaming.statistics.util.StreamUtil;

public class ReportTimer {

	private final ScheduledFuture<?> timerFuture;
	private volatile boolean reportIsDue;
	private long timeOfLastReport;

	public ReportTimer(long reportingIntervalMillis) {
		this.reportIsDue = false;
		this.timeOfLastReport = System.currentTimeMillis();

		Runnable timerRunnable = new Runnable() {
			@Override
			public void run() {
				reportIsDue = true;
			}
		};

		timerFuture = StreamUtil.scheduledAtFixedRate(timerRunnable,
				reportingIntervalMillis, reportingIntervalMillis,
				TimeUnit.MILLISECONDS);
	}

	public boolean reportIsDue() {
		return reportIsDue;
	}

	public void shutdown() {
		timerFuture.cancel(true);
	}

	public long getTimeOfLastReport() {
		return timeOfLastReport;
	}

	public void reset(long now) {
		timeOfLastReport = now;
		reportIsDue = false;
	}
}
