/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Value Object linking a number of keys to their {@link ExpiryChangeState} retaining the order of the original source.
 * Dedicated higher level methods interpret raw values retrieved from a Redis Client.
 * <ol>
 * <li>{@link #ok()} returns keys for which the time to live has been set</li>
 * <li>{@link #expired()} returns keys that have been expired</li>
 * <li>{@link #missed()} returns keys for which the time to live could not be set because they do not exist</li>
 * <li>{@link #skipped()} returns keys for which the time to live has not been set because a precondition was not
 * met</li>
 * </ol>
 *
 * @author Christoph Strobl
 * @since 3.5
 */
public class ExpireChanges<K> {

	private final Map<K, ExpiryChangeState> changes;

	ExpireChanges(Map<K, ExpiryChangeState> changes) {
		this.changes = changes;
	}

	/**
	 * Factory Method to create {@link ExpireChanges} from raw sources.
	 *
	 * @param fields the fields to associated with the raw values in states. Defines the actual order of entries within
	 *          {@link ExpireChanges}.
	 * @param states the raw Redis state change values.
	 * @return new instance of {@link ExpireChanges}.
	 * @param <K> the key type used
	 */
	public static <K> ExpireChanges<K> of(List<K> fields, List<Long> states) {

		Assert.isTrue(fields.size() == states.size(), "Keys and States must have the same number of elements");

		if (fields.size() == 1) {
			return new ExpireChanges<>(Map.of(fields.iterator().next(), stateFromValue(states.iterator().next())));
		}

		Map<K, ExpiryChangeState> target = CollectionUtils.newLinkedHashMap(fields.size());
		for (int i = 0; i < fields.size(); i++) {
			target.put(fields.get(i), stateFromValue(states.get(i)));
		}
		return new ExpireChanges<>(Collections.unmodifiableMap(target));
	}

	/**
	 * @return an ordered {@link List} of the status changes.
	 */
	public List<ExpiryChangeState> stateChanges() {
		return List.copyOf(changes.values());
	}

	/**
	 * @return the status change for the given {@literal key}, or {@literal null} if {@link ExpiryChangeState} does not
	 *         contain an entry for it.
	 */
	public @Nullable ExpiryChangeState stateOf(K key) {
		return changes.get(key);
	}

	/**
	 * @return {@literal true} if all changes are {@link ExpiryChangeState#OK}.
	 */
	public boolean allOk() {
		return allMach(ExpiryChangeState.OK::equals);
	}

	/**
	 * @return {@literal true} if all changes are either ok {@link ExpiryChangeState#OK} or
	 *         {@link ExpiryChangeState#EXPIRED}.
	 */
	public boolean allChanged() {
		return allMach(it -> ExpiryChangeState.OK.equals(it) || ExpiryChangeState.EXPIRED.equals(it));
	}

	/**
	 * @return an ordered list of if all changes are {@link ExpiryChangeState#OK}.
	 */
	public Set<K> ok() {
		return filterByState(ExpiryChangeState.OK);
	}

	/**
	 * @return an ordered list of if all changes are {@link ExpiryChangeState#EXPIRED}.
	 */
	public Set<K> expired() {
		return filterByState(ExpiryChangeState.EXPIRED);
	}

	/**
	 * @return an ordered list of if all changes are {@link ExpiryChangeState#DOES_NOT_EXIST}.
	 */
	public Set<K> missed() {
		return filterByState(ExpiryChangeState.DOES_NOT_EXIST);
	}

	/**
	 * @return an ordered list of if all changes are {@link ExpiryChangeState#CONDITION_NOT_MET}.
	 */
	public Set<K> skipped() {
		return filterByState(ExpiryChangeState.CONDITION_NOT_MET);
	}

	public boolean allMach(Predicate<ExpiryChangeState> predicate) {
		return changes.values().stream().allMatch(predicate);
	}

	private Set<K> filterByState(ExpiryChangeState filter) {
		return changes.entrySet().stream().filter(entry -> entry.getValue().equals(filter)).map(Map.Entry::getKey)
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	private static ExpiryChangeState stateFromValue(Number value) {
		return ExpiryChangeState.of(value);
	}

	public record ExpiryChangeState(long value) {

		public static final ExpiryChangeState DOES_NOT_EXIST = new ExpiryChangeState(-2L);
		public static final ExpiryChangeState CONDITION_NOT_MET = new ExpiryChangeState(0L);
		public static final ExpiryChangeState OK = new ExpiryChangeState(1L);
		public static final ExpiryChangeState EXPIRED = new ExpiryChangeState(2L);

		static ExpiryChangeState of(boolean value) {
			return value ? OK : CONDITION_NOT_MET;
		}

		static ExpiryChangeState of(Number value) {
			return switch (value.intValue()) {
				case -2 -> DOES_NOT_EXIST;
				case 0 -> CONDITION_NOT_MET;
				case 1 -> OK;
				case 2 -> EXPIRED;
				default -> new ExpiryChangeState(value.longValue());
			};
		}

		public boolean isOk() {
			return OK.equals(this);
		}

		public boolean isExpired() {
			return EXPIRED.equals(this);
		}

		public boolean isMissing() {
			return DOES_NOT_EXIST.equals(this);
		}

		public boolean isSkipped() {
			return CONDITION_NOT_MET.equals(this);
		}

		@Override
		public boolean equals(Object o) {

			if (o == this) {
				return true;
			}

			if (!(o instanceof ExpiryChangeState that)) {
				return false;
			}

			return this.value == that.value;
		}

		@Override
		public int hashCode() {
			return Objects.hash(value);
		}
	}
}
