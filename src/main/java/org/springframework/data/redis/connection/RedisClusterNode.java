/*
 * Copyright 2015-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Representation of a Redis server within the cluster.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisClusterNode extends RedisNode {

	/**
	 * Get {@link RedisClusterNodeBuilder} for creating new {@link RedisClusterNode}.
	 *
	 * @return never {@literal null}.
	 */
	public static RedisClusterNodeBuilder newRedisClusterNode() {
		return new RedisClusterNodeBuilder();
	}

	private @Nullable LinkState linkState;

	private Set<Flag> flags;

	private final SlotRange slotRange;

	protected RedisClusterNode() {

		this.flags = Collections.emptySet();
		this.slotRange = SlotRange.empty();
	}

	/**
	 * Creates new {@link RedisClusterNode} with empty {@link SlotRange}.
	 *
	 * @param host must not be {@literal null}.
	 * @param port
	 */
	public RedisClusterNode(String host, int port) {
		this(host, port, SlotRange.empty());
	}

	/**
	 * Creates new {@link RedisClusterNode} with an id and empty {@link SlotRange}.
	 *
	 * @param id must not be {@literal null}.
	 */
	public RedisClusterNode(String id) {

		this(SlotRange.empty());

		Assert.notNull(id, "Id must not be null");

		this.id = id;
	}

	/**
	 * Creates new {@link RedisClusterNode} with given {@link SlotRange}.
	 *
	 * @param slotRange must not be {@literal null}.
	 */
	public RedisClusterNode(SlotRange slotRange) {

		Assert.notNull(slotRange, "SlotRange must not be null");

		this.flags = Collections.emptySet();
		this.slotRange = slotRange;
	}

	/**
	 * Creates new {@link RedisClusterNode} with given {@link SlotRange}.
	 *
	 * @param host must not be {@literal null}.
	 * @param port
	 * @param slotRange must not be {@literal null}.
	 */
	public RedisClusterNode(String host, int port, SlotRange slotRange) {

		super(host, port);

		Assert.notNull(slotRange, "SlotRange must not be null");

		this.flags = Collections.emptySet();
		this.slotRange = slotRange;
	}

	/**
	 * Get the served {@link SlotRange}.
	 *
	 * @return never {@literal null}.
	 */
	public SlotRange getSlotRange() {
		return this.slotRange;
	}

	/**
	 * Determines whether this {@link RedisClusterNode} manages the identified {@link Integer slot} in the cluster.
	 *
	 * @param slot {@link Integer} identifying the slot to evaluate.
	 * @return {@literal true} if slot is covered.
	 */
	public boolean servesSlot(int slot) {
		return getSlotRange().contains(slot);
	}

	/**
	 * @return can be {@literal null}
	 */
	public @Nullable LinkState getLinkState() {
		return this.linkState;
	}

	/**
	 * @return true if node is connected to cluster.
	 */
	public boolean isConnected() {
		return LinkState.CONNECTED.equals(getLinkState());
	}

	/**
	 * @return never {@literal null}.
	 */
	@SuppressWarnings("all")
	public Set<Flag> getFlags() {
		return this.flags != null ? this.flags : Collections.emptySet();
	}

	/**
	 * @return true if node is marked as failing.
	 */
	public boolean isMarkedAsFail() {
		return CollectionUtils.containsAny(getFlags(), Arrays.asList(Flag.FAIL, Flag.PFAIL));
	}

	@Override
	public String toString() {
		return super.toString();
	}

	/**
	 * @author Christoph Strobl
	 * @author daihuabin
	 * @author John Blum
	 * @since 1.7
	 */
	public static class SlotRange {

		/**
		 * Factory method used to construct a new, empty {@link SlotRange}.
		 *
		 * @return a new, empty {@link SlotRange}.
		 */
		public static SlotRange empty() {
			return new SlotRange(Collections.emptySet());
		}

		private final BitSet range;

		/**
		 * @param lowerBound must not be {@literal null}.
		 * @param upperBound must not be {@literal null}.
		 */
		public SlotRange(Integer lowerBound, Integer upperBound) {

			Assert.notNull(lowerBound, "LowerBound must not be null");
			Assert.notNull(upperBound, "UpperBound must not be null");

			this.range = new BitSet(upperBound + 1);

			for (int bitindex = lowerBound; bitindex <= upperBound; bitindex++) {
				this.range.set(bitindex);
			}
		}

		public SlotRange(Collection<Integer> range) {

			if (CollectionUtils.isEmpty(range)) {
				this.range = new BitSet(0);
			} else {
				this.range = new BitSet(ClusterSlotHashUtil.SLOT_COUNT);
				for (Integer bitindex : range) {
					this.range.set(bitindex);
				}
			}
		}

		public SlotRange(BitSet range) {
			this.range = (BitSet) range.clone();
		}

		/**
		 * Determines whether this {@link SlotRange} contains the given {@link Integer slot}, which implies this cluster
		 * nodes manages the slot holding data stored in Redis.
		 *
		 * @param slot {@link Integer slot} to evaluate.
		 * @return true when slot is part of the range.
		 */
		public boolean contains(int slot) {
			return this.range.get(slot);
		}

		/**
		 * Gets all slots in this {@link SlotRange} managed by this cluster node.
		 *
		 * @return all slots in this {@link SlotRange}.
		 */
		public Set<Integer> getSlots() {

			if (this.range.isEmpty()) {
				return Collections.emptySet();
			}

			Set<Integer> slots = new LinkedHashSet<>(Math.max(2 * this.range.cardinality(), 11));

			for (int bitindex = 0; bitindex < this.range.length(); bitindex++) {
				if (this.range.get(bitindex)) {
					slots.add(bitindex);
				}
			}

			return Collections.unmodifiableSet(slots);
		}

		public int[] getSlotsArray() {

			if (this.range.isEmpty()) {
				return new int[0];
			}

			int[] slots = new int[this.range.cardinality()];
			int arrayIndex = 0;

			for (int slot = 0; slot < ClusterSlotHashUtil.SLOT_COUNT; slot++) {
				if (this.range.get(slot)) {
					slots[arrayIndex++] = slot;
				}
			}

			return slots;
		}

		@Override
		public String toString() {
			return Arrays.toString(getSlotsArray());
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public enum LinkState {
		CONNECTED, DISCONNECTED
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public enum Flag {

		MYSELF("myself"), MASTER("master"), REPLICA("slave"), FAIL("fail"), PFAIL("fail?"), HANDSHAKE("handshake"), NOADDR(
				"noaddr"), NOFLAGS("noflags");

		private String raw;

		Flag(String raw) {
			this.raw = raw;
		}

		public String getRaw() {
			return this.raw;
		}
	}

	/**
	 * Builder for creating new {@link RedisClusterNode}.
	 *
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	public static class RedisClusterNodeBuilder extends RedisNodeBuilder {

		@Nullable Set<Flag> flags;
		@Nullable LinkState linkState;
		SlotRange slotRange;

		public RedisClusterNodeBuilder() {
			this.slotRange = SlotRange.empty();
		}

		@Override
		public RedisClusterNodeBuilder listeningAt(String host, int port) {
			super.listeningAt(host, port);
			return this;
		}

		@Override
		public RedisClusterNodeBuilder withName(String name) {
			super.withName(name);
			return this;
		}

		@Override
		public RedisClusterNodeBuilder withId(String id) {
			super.withId(id);
			return this;
		}

		@Override
		public RedisClusterNodeBuilder promotedAs(NodeType nodeType) {
			super.promotedAs(nodeType);
			return this;
		}

		@Override
		public RedisClusterNodeBuilder replicaOf(String masterId) {
			super.replicaOf(masterId);
			return this;
		}

		/**
		 * Set flags for node.
		 *
		 * @param flags
		 * @return
		 */
		public RedisClusterNodeBuilder withFlags(Set<Flag> flags) {

			this.flags = flags;
			return this;
		}

		/**
		 * Set {@link SlotRange}.
		 *
		 * @param range
		 * @return
		 */
		public RedisClusterNodeBuilder serving(SlotRange range) {

			this.slotRange = range;
			return this;
		}

		/**
		 * Set {@link LinkState}.
		 *
		 * @param linkState
		 * @return
		 */
		public RedisClusterNodeBuilder linkState(LinkState linkState) {
			this.linkState = linkState;
			return this;
		}

		@Override
		@SuppressWarnings("NullAway")
		public RedisClusterNode build() {

			RedisNode base = super.build();

			RedisClusterNode node;
			if (base.getHost() != null) {
				node = new RedisClusterNode(base.getRequiredHost(), base.getRequiredPort(), slotRange);
			} else {
				node = new RedisClusterNode(slotRange);
			}
			node.id = base.id;
			node.type = base.type;
			node.masterId = base.masterId;
			node.name = base.name;
			node.flags = flags != null ? flags : Collections.emptySet();
			node.linkState = linkState;
			return node;
		}
	}

}
