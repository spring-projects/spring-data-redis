/*
 * Copyright 2017-2025 the original author or authors.
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
 *  limitations under the License.
 */
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RedisClusterNode.SlotRange}.
 *
 * @author John Blum
 * @see org.junit.jupiter.api.Test
 * @see org.springframework.data.redis.connection.RedisClusterNode.SlotRange
 * @since 3.2.0
 */
public class RedisClusterNodeSlotRangeUnitTests {

	@Test // GH-2525
	public void containsFromLowerToUpperBoundSlotsOnly() {

		RedisClusterNode.SlotRange slotRange = new RedisClusterNode.SlotRange(25, 75);

		IntStream.range(0, 100).forEach(slot ->
			assertThat(slotRange.contains(slot)).isEqualTo(slot >= 25 && slot <= 75));
	}

	@Test // GH-2525
	public void containsSpecificSlots() {

		Set<Integer> slots = Set.of(1, 2, 4, 8, 16, 32, 64);

		RedisClusterNode.SlotRange slotRange = new RedisClusterNode.SlotRange(slots);

		IntStream.range(0, 100).forEach(slot ->
			assertThat(slotRange.contains(slot)).isEqualTo(slots.contains(slot)));
	}

	@Test // GH-2525
	public void emptySlotRange() {

		RedisClusterNode.SlotRange slotRange = RedisClusterNode.SlotRange.empty();

		assertThat(slotRange).isNotNull();
		assertThat(slotRange.getSlots()).isEmpty();
		assertThat(slotRange).hasToString("[]");
	}

	@Test // GH-2525
	public void slotRangeSlotsAreCorrect() {

		assertThat(new RedisClusterNode.SlotRange(4, 6).getSlots())
			.containsExactlyInAnyOrder(4, 5, 6);

		assertThat(new RedisClusterNode.SlotRange(Arrays.asList(1, 2, 3, 5, 7)).getSlots())
			.containsExactlyInAnyOrder(1, 2, 3, 5, 7);
	}

	@Test // GH-2525
	public void slotRangeSlotsArrayIsCorrectIsCorrect() {

		assertThat(new RedisClusterNode.SlotRange(4, 6).getSlotsArray())
			.containsExactly(4, 5, 6);

		assertThat(new RedisClusterNode.SlotRange(Arrays.asList(1, 2, 3, 5, 7)).getSlotsArray())
			.containsExactly(1, 2, 3, 5, 7);
	}

	@Test // GH-2525
	public void toStringListsSlots() {
		assertThat(new RedisClusterNode.SlotRange(List.of(1, 2, 4, 8, 16, 32, 64)))
			.hasToString("[1, 2, 4, 8, 16, 32, 64]");
	}
}
