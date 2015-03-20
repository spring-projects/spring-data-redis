/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection;

/**
 * @author Christoph Strobl
 * @since 1.5
 */
public class RedisClusterNode extends RedisNode {

	private final SlotRange slotRange;

	public RedisClusterNode(String host, int port, SlotRange slotRange) {
		super(host, port);
		this.slotRange = slotRange;
	}

	public SlotRange getSlotRange() {
		return slotRange;
	}

	public boolean servesSlot(int slot) {
		return slotRange.contains(slot);
	}

	@Override
	public String toString() {
		return "RedisClusterNode [" + getHost() + ":" + getPort() + ", slotRange=" + slotRange + "]";
	}

	public static class SlotRange {

		private final Integer lowerBound;
		private final Integer upperBound;

		public SlotRange(Integer lowerBound, Integer upperBound) {
			super();
			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
		}

		public Integer getLowerBound() {
			return lowerBound;
		}

		public Integer getUpperBound() {
			return upperBound;
		}

		@Override
		public String toString() {
			return "[" + lowerBound + "-" + upperBound + "]";
		}

		public boolean contains(int slot) {
			return lowerBound <= slot && slot < upperBound;
		}

	}
}
