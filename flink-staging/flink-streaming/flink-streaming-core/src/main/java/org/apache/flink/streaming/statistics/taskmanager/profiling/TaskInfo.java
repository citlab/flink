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

package org.apache.flink.streaming.statistics.taskmanager.profiling;

import akka.actor.UntypedActor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.profiling.impl.EnvironmentThreadSet;
import org.apache.flink.runtime.profiling.impl.types.InternalExecutionVertexThreadProfilingData;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosStatistic;
import org.apache.flink.streaming.statistics.taskmanager.qosmodel.QosValue;

import java.lang.management.ThreadMXBean;

/**
 * @author Bjoern Lohrmann, Sascha Wolke
 */
public class TaskInfo extends UntypedActor {

	public static final int CPU_STATISTIC_WINDOW_SIZE = 5;

	private final Task task;

	private final ThreadMXBean tmx;

	private QosStatistic cpuUtilization;

	private double unchainedCpuUtilization;

	private volatile EnvironmentThreadSet environmentThreadSet;

	private boolean isChained;

	private TaskInfo nextInChain;

	public TaskInfo(Task task, ThreadMXBean tmx) {
		this.task = task;
		this.tmx = tmx;
		this.cpuUtilization = new QosStatistic(CPU_STATISTIC_WINDOW_SIZE);
		this.unchainedCpuUtilization = -1;
		this.task.registerExecutionListener(getSelf());
		this.isChained = false;
		this.nextInChain = null;
	}

	public Task getTask() {
		return this.task;
	}

	public ExecutionAttemptID getExecutionId() {
		return this.task.getExecutionId();
	}

	public synchronized void measureCpuUtilization() {

		if (this.environmentThreadSet != null) {

			long now = System.currentTimeMillis();
			InternalExecutionVertexThreadProfilingData profilingData = this.environmentThreadSet
					.captureCPUUtilization(this.task.getJobID(), this.tmx, now);

			/**
			 * cpuUtilization measures in percent, how much of one CPU core's
			 * available time the the vertex's main thread AND its associated
			 * user threads have consumed. Example values:
			 * 
			 * cpuUtilization == 50 => it uses half a core's worth of CPU time
			 * 
			 * cpuUtilization == 200 => it uses two core's worth of CPU time (this
			 * can happen if the vertex's main thread spawns several user
			 * threads)
			 */
			double cpuUtilization = (profilingData.getUserTime()
					+ profilingData.getSystemTime() + profilingData
						.getBlockedTime())
					* (this.environmentThreadSet.getNumberOfUserThreads() + 1);

			this.cpuUtilization.addValue(new QosValue(cpuUtilization, now));
		}
	}

	public boolean hasCPUUtilizationMeasurements() {
		return this.cpuUtilization.hasValues();
	}

	public synchronized double getCPUUtilization() {
		return this.cpuUtilization.getMean();
	}

	public synchronized double getUnchainedCpuUtilization() {
		return this.unchainedCpuUtilization;
	}

	public void executionStateChanged(ExecutionState newExecutionState) {

		switch (newExecutionState) {
		case RUNNING:
			this.environmentThreadSet =	new EnvironmentThreadSet(
					this.tmx,
					this.task.getEnvironment().getExecutingThread(),
					this.task.getVertexID(),
					this.task.getSubtaskIndex(),
					this.task.getExecutionId());
			break;
		case FINISHED:
		case CANCELING:
		case CANCELED:
		case FAILED:
		case SUSPENDING:
		case SUSPENDED:
			this.environmentThreadSet = null;
			break;
		default:
			break;
		}
	}

	@Override
	public void onReceive(Object message) throws Exception {
		if (message instanceof ExecutionGraphMessages.ExecutionStateChanged) {
			executionStateChanged(
				((ExecutionGraphMessages.ExecutionStateChanged) message).newExecutionState());

		} else {
			unhandled(message);
		}
	}

	public void cleanUp() {
		this.task.unregisterExecutionListener(getSelf());
	}

	public synchronized void setIsChained(boolean isChained) {
		if (!this.isChained && this.hasCPUUtilizationMeasurements()) {
			this.unchainedCpuUtilization = this.getCPUUtilization();
		}

		if (this.hasCPUUtilizationMeasurements()) {
			this.cpuUtilization = new QosStatistic(CPU_STATISTIC_WINDOW_SIZE);
		}

		this.isChained = isChained;
	}

	public synchronized void setNextInChain(TaskInfo nextInChain) {
		this.nextInChain = nextInChain;
	}

	public synchronized TaskInfo getNextInChain() {
		return this.nextInChain;
	}

	public synchronized boolean isChained() {
		return this.isChained;
	}
}
