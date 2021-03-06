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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import java.io.IOException;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound
 * handler about non-emptiness, similar to the {@link LocalInputChannel}.
 */
class CreditBasedSequenceNumberingViewReader implements BufferAvailabilityListener, NetworkSequenceViewReader {

	private final Object requestLock = new Object();

	//对应的 RemoteInputChannel 的 ID
	private final InputChannelID receiverId;

	private final PartitionRequestQueue requestQueue;

	//消费 ResultSubpartition 的数据，并在 ResultSubpartition 有数据可用时获得通知
	private volatile ResultSubpartitionView subpartitionView;


	/**
	 * The status indicating whether this reader is already enqueued in the pipeline for transferring
	 * data or not.
	 *
	 * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
	 * thread only since there is no synchronisation.
	 */
	private boolean isRegisteredAsAvailable = false;

	/**
	 * The number of available buffers for holding data on the consumer side.
	 */
	//numCreditsAvailable的值是消费端还能够容纳的buffer的数量，也就是允许生产端发送的buffer的数量
	private int numCreditsAvailable;

	//序列号，自增
	private int sequenceNumber = -1;

	CreditBasedSequenceNumberingViewReader(
		InputChannelID receiverId,
		int initialCredit,
		PartitionRequestQueue requestQueue) {

		this.receiverId = receiverId;
		this.numCreditsAvailable = initialCredit;
		this.requestQueue = requestQueue;
	}

	//创建一个 ResultSubpartitionView，用于读取数据，并在有数据可用时获得通知
	@Override
	public void requestSubpartitionView(
		ResultPartitionProvider partitionProvider,
		ResultPartitionID resultPartitionId,
		int subPartitionIndex) throws IOException {

		synchronized (requestLock) {
			if (subpartitionView == null) {
				// This this call can trigger a notification we have to
				// schedule a separate task at the event loop that will
				// start consuming this. Otherwise the reference to the
				// view cannot be available in getNextBuffer().
				this.subpartitionView = partitionProvider.createSubpartitionView(
					resultPartitionId,
					subPartitionIndex,
					this);
			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	@Override
	public void addCredit(int creditDeltas) {
		numCreditsAvailable += creditDeltas;
	}

	@Override
	public void resumeConsumption() {
		subpartitionView.resumeConsumption();
	}

	@Override
	public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
		this.isRegisteredAsAvailable = isRegisteredAvailable;
	}

	@Override
	public boolean isRegisteredAsAvailable() {
		return isRegisteredAsAvailable;
	}

	/**
	 * Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 */
	//是否还可以消费数据：
	// 1. ResultSubpartition 中有更多的数据
	// 2. credit > 0 或者下一条数据是事件(事件不需要消耗credit)
	@Override
	public boolean isAvailable() {
		// BEWARE: this must be in sync with #isAvailable(BufferAndBacklog)!
		return subpartitionView.isAvailable(numCreditsAvailable);
	}

	/**
	 * Check whether this reader is available or not (internal use, in sync with
	 * {@link #isAvailable()}, but slightly faster).
	 *
	 * <p>Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 *
	 * @param bufferAndBacklog current buffer and backlog including information about the next buffer
	 */
	private boolean isAvailable(BufferAndBacklog bufferAndBacklog) {
		// BEWARE: this must be in sync with #isAvailable()!
		if (numCreditsAvailable > 0) {
			return bufferAndBacklog.isDataAvailable();
		} else {
			return bufferAndBacklog.isEventAvailable();
		}
	}

	@Override
	public InputChannelID getReceiverId() {
		return receiverId;
	}

	@Override
	public int getSequenceNumber() {
		return sequenceNumber;
	}

	@VisibleForTesting
	int getNumCreditsAvailable() {
		return numCreditsAvailable;
	}

	@VisibleForTesting
	boolean hasBuffersAvailable() {
		return subpartitionView.isAvailable(Integer.MAX_VALUE);
	}

	/**
	 * 读取数据
	 *
	 * @return
	 * @throws IOException
	 */
	@Override
	public BufferAndAvailability getNextBuffer() throws IOException {
		BufferAndBacklog next = subpartitionView.getNextBuffer(); // 读取数据
		if (next != null) {
			sequenceNumber++; // 序列号

			// 要发送一个 buffer，对应的 numCreditsAvailable 要减1
			if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
				throw new IllegalStateException("no credit available");
			}

			return new BufferAndAvailability(
				next.buffer(), isAvailable(next), next.buffersInBacklog());
		} else {
			return null;
		}
	}

	@Override
	public boolean isReleased() {
		return subpartitionView.isReleased();
	}

	@Override
	public Throwable getFailureCause() {
		return subpartitionView.getFailureCause();
	}

	@Override
	public void releaseAllResources() throws IOException {
		subpartitionView.releaseAllResources();
	}

	//在 ResultSubpartition 中有数据时会回调该方法
	@Override
	public void notifyDataAvailable() {
		//告知 PartitionRequestQueue 当前 ViewReader 有数据可读
		requestQueue.notifyReaderNonEmpty(this);
	}

	@Override
	public String toString() {
		return "CreditBasedSequenceNumberingViewReader{" +
			"requestLock=" + requestLock +
			", receiverId=" + receiverId +
			", sequenceNumber=" + sequenceNumber +
			", numCreditsAvailable=" + numCreditsAvailable +
			", isRegisteredAsAvailable=" + isRegisteredAsAvailable +
			'}';
	}
}
