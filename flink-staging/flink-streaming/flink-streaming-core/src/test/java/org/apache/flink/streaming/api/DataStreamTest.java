/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.statistics.ConstraintUtil;
import org.apache.flink.streaming.statistics.JobGraphLatencyConstraint;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class DataStreamTest {

	/**
	 * Generate a graph with two sequences => two constraints.
	 *
	 *                       -> mapB1 -> mapB2 ->
	 * source -> afterSource -> mapA -----------> coFlatMap -> sink
	 *        ^ 										 												^
	 *     constraint																		constraint
	 *     begin																				end
	 */
	@Test
	public void testConstraint() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> source = env.fromElements(WordCountData.STREAMING_COUNTS_AS_TUPLES);

		DataStream<String> afterSource = source
				.beginLatencyConstraint(100, true)
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
						return "afterSource";
					}
				});

		DataStream<String> mapA = afterSource
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
						return "mapA";
					}
				});

		DataStream<String> mapB1 = afterSource
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
						return "mapB1";
					}
				});

		DataStream<String> mapB2 = mapB1
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
						return "mapB2";
					}
				});

		SingleOutputStreamOperator<String, ?> coFlatMap = mapA
				.connect(mapB2)
				.flatMap(new CoFlatMapFunction<String, String, String>() {
					@Override
					public void flatMap1(String value, Collector<String> out) throws Exception {
						out.collect("coFlatMap");
					}

					@Override
					public void flatMap2(String value, Collector<String> out) throws Exception {
						out.collect("coFlatMap");
					}
				});

		coFlatMap
				.finishLatencyConstraint()
				.addSink(new SinkFunction<String>() {
					@Override
					public void invoke(String value) throws Exception {
						// do nothing
					}
				});

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setChaining(false);
		JobGraph jobGraph = streamGraph.getJobGraph();

		List<JobGraphLatencyConstraint> constraints = ConstraintUtil.getConstraints(jobGraph.getJobConfiguration());
		assertEquals(2, constraints.size());
	}
}
