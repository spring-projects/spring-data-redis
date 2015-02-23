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

	public boolean servesSlot(Long slot) {
		return slotRange.contains(slot);
	}

	@Override
	public String toString() {
		return "RedisClusterNode [" + getHost() + ":" + getPort() + ", slotRange=" + slotRange + "]";
	}

	public static class SlotRange {

		private final Long lowerBound;
		private final Long upperBound;

		public SlotRange(Long lowerBound, Long upperBound) {
			super();
			this.lowerBound = lowerBound;
			this.upperBound = upperBound;
		}

		public Long getLowerBound() {
			return lowerBound;
		}

		public Long getUpperBound() {
			return upperBound;
		}

		@Override
		public String toString() {
			return "[" + lowerBound + "-" + upperBound + "]";
		}

		public boolean contains(Long slot) {
			return lowerBound <= slot && slot < upperBound;
		}

	}
}
