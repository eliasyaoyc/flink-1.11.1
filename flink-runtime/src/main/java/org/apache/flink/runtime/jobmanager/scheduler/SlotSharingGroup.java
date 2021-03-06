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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A slot sharing units defines which different task (from different job vertices) can be
 * deployed together within a slot. This is a soft permission, in contrast to the hard constraint
 * defined by a co-location hint.
 *
 * 相同的 SlotSharingGroup 的不同 JobVertex 的子任务可以被分配在同一个 slot 中，但不保证能做到。
 * 对于普通的 SlotSharingGroup 的约束，行程的树形结构时：MultiTaskSlot 作为根节点，多个 SingleTaskSlot 作为叶子节点，这些叶子节点分配代表不同的任务，用来区分它们的JobVertexId 不同。
 * 对于 CoLocationGroup 强制约束，会在 MultiTaskSlot 根节点的下一级创建一个 MultiTaskSlot 节点（用 CoLocationGroup ID） 来区分，同一个 CoLocationGroup 约束下的子任务进一步作为第二层 MultiTaskSlot 的叶子节点。
 */
public class SlotSharingGroup implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private final Set<JobVertexID> ids = new TreeSet<>();

	private final SlotSharingGroupId slotSharingGroupId = new SlotSharingGroupId();

	/** Represents resources of all tasks in the group. Default to be zero.
	 * Any task with UNKNOWN resources will turn it to be UNKNOWN. */
	private ResourceSpec resourceSpec = ResourceSpec.ZERO;

	// --------------------------------------------------------------------------------------------

	public void addVertexToGroup(final JobVertexID id, final ResourceSpec resource) {
		ids.add(checkNotNull(id));
		resourceSpec = resourceSpec.merge(checkNotNull(resource));
	}

	public void removeVertexFromGroup(final JobVertexID id, final ResourceSpec resource) {
		ids.remove(checkNotNull(id));
		resourceSpec = resourceSpec.subtract(checkNotNull(resource));
	}

	public Set<JobVertexID> getJobVertexIds() {
		return Collections.unmodifiableSet(ids);
	}

	public SlotSharingGroupId getSlotSharingGroupId() {
		return slotSharingGroupId;
	}

	public ResourceSpec getResourceSpec() {
		return resourceSpec;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "SlotSharingGroup " + this.ids.toString();
	}
}
