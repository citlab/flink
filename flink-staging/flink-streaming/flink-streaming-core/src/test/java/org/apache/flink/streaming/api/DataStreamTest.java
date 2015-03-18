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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.constraint.StreamGraphConstraint;
import org.apache.flink.streaming.api.constraint.identifier.ConstraintIdentifier;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoFlatMapFunction;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.util.Collector;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class DataStreamTest {
	@Test
	public void testConstraint() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStreamSource<String> source = env.fromElements(WordCountData.STREAMING_COUNTS_AS_TUPLES);

		DataStream<String> map = source
				.beginLatencyConstraint(100)
				.map(new MapFunction<String, String>() {
					@Override
					public String map(String value) throws Exception {
						return "map";
					}
				});

		SingleOutputStreamOperator<String, ?> flatMap = source
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public void flatMap(String value, Collector<String> out) throws Exception {
						out.collect("flatMap");
					}
				});

		flatMap
				.connect(map)
				.flatMap(new CoFlatMapFunction<String, String, String>() {
					@Override
					public void flatMap1(String value, Collector<String> out) throws Exception {
						out.collect("coFlatMap");
					}

					@Override
					public void flatMap2(String value, Collector<String> out) throws Exception {
						out.collect("coFlatMap");
					}
				})
				.finishLatencyConstraint();

		StreamGraph streamGraph = env.getStreamGraph();

		streamGraph.calculateConstraints();
		Map<ConstraintIdentifier, StreamGraphConstraint> constraints = streamGraph.getConstraints();
		assertEquals(1, constraints.size());

		StreamGraphConstraint constraint = constraints.values().iterator().next();
		assertEquals(100, constraint.getMaxLatency());
		assertEquals(2, constraint.getSequences().size());
	}
}
