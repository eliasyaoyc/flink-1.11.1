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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.fail;

/**
 * Test base for fault tolerant streaming programs.
 */
@RunWith(Parameterized.class)
public abstract class StreamFaultToleranceTestBase extends TestLogger {

	@Parameterized.Parameters(name = "FailoverStrategy: {0}")
	public static Collection<FailoverStrategy> parameters() {
		return Arrays.asList(FailoverStrategy.RestartAllFailoverStrategy, FailoverStrategy.RestartPipelinedRegionFailoverStrategy);
	}

	/**
	 * The failover strategy to use.
	 */
	public enum FailoverStrategy{
		RestartAllFailoverStrategy,
		RestartPipelinedRegionFailoverStrategy
	}

	@Parameterized.Parameter
	public FailoverStrategy failoverStrategy;

	protected static final int NUM_TASK_MANAGERS = 3;
	protected static final int NUM_TASK_SLOTS = 4;
	protected static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

	private static MiniClusterWithClientResource cluster;

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		switch (failoverStrategy) {
			case RestartPipelinedRegionFailoverStrategy:
				configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
				break;
			case RestartAllFailoverStrategy:
				configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "full");
		}

		cluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(configuration)
				.setNumberTaskManagers(NUM_TASK_MANAGERS)
				.setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
				.build());
		cluster.before();
	}

	@After
	public void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.after();
			cluster = null;
		}
	}

	/**
	 * Implementations are expected to assemble the test topology in this function
	 * using the provided {@link StreamExecutionEnvironment}.
	 */
	public abstract void testProgram(StreamExecutionEnvironment env);

	/**
	 * Implementations are expected to provide test here to verify the correct behavior.
	 */
	public abstract void postSubmit() throws Exception;

	/**
	 * Runs the following program the test program defined in {@link #testProgram(StreamExecutionEnvironment)}
	 * followed by the checks in {@link #postSubmit}.
	 */
	@Test
	public void runCheckpointedProgram() throws Exception {
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.enableCheckpointing(500);
						env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 0L));

			testProgram(env);

			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			try {
				ClientUtils.submitJobAndWaitForResult(cluster.getClusterClient(), jobGraph, getClass().getClassLoader()).getJobExecutionResult();
			} catch (ProgramInvocationException root) {
				Throwable cause = root.getCause();

				// search for nested SuccessExceptions
				int depth = 0;
				while (!(cause instanceof SuccessException)) {
					if (cause == null || depth++ == 20) {
						root.printStackTrace();
						fail("Test failed: " + root.getMessage());
					}
					else {
						cause = cause.getCause();
					}
				}
			}

			postSubmit();
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Frequently used utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * POJO storing prefix, value, and count.
	 */
	@SuppressWarnings("serial")
	public static class PrefixCount implements Serializable {

		public String prefix;
		public String value;
		public long count;

		public PrefixCount() {}

		public PrefixCount(String prefix, String value, long count) {
			this.prefix = prefix;
			this.value = value;
			this.count = count;
		}

		@Override
		public String toString() {
			return prefix + " / " + value;
		}
	}
}
