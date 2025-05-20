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
package org.springframework.data.redis.core.types;

import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

/**
 * Value Object linking a number of keys to their {@link TimeToLive} retaining the order of the original source.
 * Dedicated higher level methods interpret raw expiration values retrieved from a Redis Client.
 * <ol>
 * <li>{@link #persistent()} returns keys that do not have an associated time to live</li>
 * <li>{@link #missing()} returns keys that do not exist and therefore have no associated time to live</li>
 * <li>{@link #ttl()} returns the ordered list of {@link TimeToLive expirations} based on the raw values</li>
 * <li>{@link #expiring()} returns the expiring keys along with their {@link Duration time to live}</li>
 * </ol>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 3.5
 */
public class Expirations<K> {

	private final TimeUnit unit;
	private final Map<K, TimeToLive> expirations;

	Expirations(TimeUnit unit, Map<K, TimeToLive> expirations) {
		this.unit = unit;
		this.expirations = expirations;
	}

	/**
	 * Factory Method to create {@link Expirations} from raw sources provided in a given {@link TimeUnit}.
	 *
	 * @param targetUnit the actual time unit of the raw timeToLive values.
	 * @param keys the keys to associated with the raw values in timeToLive. Defines the actual order of entries within
	 *          {@link Expirations}.
	 * @param timeouts the raw Redis time to live values.
	 * @return new instance of {@link Expirations}.
	 * @param <K> the key type used
	 */
	public static <K> Expirations<K> of(TimeUnit targetUnit, List<K> keys, Timeouts timeouts) {

		if (keys.size() != timeouts.size()) {
			throw new IllegalArgumentException(
					"Keys and Timeouts must be of same size but was %s vs %s".formatted(keys.size(), timeouts.size()));
		}
		if (keys.size() == 1) {
			return new Expirations<>(targetUnit,
					Map.of(keys.iterator().next(), TimeToLive.of(timeouts.raw().iterator().next(), timeouts.timeUnit())));
		}

		Map<K, TimeToLive> target = CollectionUtils.newLinkedHashMap(keys.size());
		for (int i = 0; i < keys.size(); i++) {
			target.put(keys.get(i), TimeToLive.of(timeouts.get(i), timeouts.timeUnit()));
		}
		return new Expirations<>(targetUnit, target);
	}

	/**
	 * @return an ordered set of keys that do not have a time to live.
	 */
	public Set<K> persistent() {
		return filterByState(TimeToLive.PERSISTENT);
	}

	/**
	 * @return an ordered set of keys that do not exist and therefore do not have a time to live.
	 */
	public Set<K> missing() {
		return filterByState(TimeToLive.MISSING);
	}

	/**
	 * @return an ordered set of all {@link Expirations expirations} where the {@link TimeToLive#value()} is using the
	 *         {@link TimeUnit} defined in {@link #timeUnit()}.
	 */
	public List<TimeToLive> ttl() {
		return expirations.values().stream().map(it -> it.convert(this.unit)).toList();
	}

	/**
	 * @return the {@link TimeUnit} for {@link TimeToLive expirations} held by this instance.
	 */
	public TimeUnit timeUnit() {
		return unit;
	}

	/**
	 * @return an ordered {@link List} of {@link java.util.Map.Entry entries} combining keys with their actual time to
	 *         live. {@link TimeToLive#isMissing() Missing} and {@link TimeToLive#isPersistent() persistent} entries are
	 *         skipped.
	 */
	public List<Map.Entry<K, Duration>> expiring() {

		return expirations.entrySet().stream().filter(it -> !it.getValue().isMissing() && !it.getValue().isPersistent())
				.map(it -> Map.entry(it.getKey(), toDuration(it.getValue()))).toList();
	}

	/**
	 * @return the ordered collection of keys that are associated with an expiration.
	 */
	public Collection<K> keys() {
		return expirations.keySet();
	}

	/**
	 * @param key
	 * @return the {@link Expirations expirations} where the {@link TimeToLive#value()} is using the {@link TimeUnit}
	 *         defined in {@link #timeUnit()} or {@literal null} if no entry could be found.
	 */
	public @Nullable TimeToLive expirationOf(K key) {

		TimeToLive timeToLive = expirations.get(key);

		if (timeToLive == null) {
			return null;
		}

		return timeToLive.convert(this.unit);
	}

	/**
	 * @param key
	 * @return the time to live value of the requested key if it exists and the expiration is neither
	 *         {@link TimeToLive#isMissing() missing} nor {@link TimeToLive#isPersistent() persistent}, {@literal null}
	 *         otherwise.
	 */
	public @Nullable Duration ttlOf(K key) {
		return toDuration(expirationOf(key));
	}

	private Set<K> filterByState(TimeToLive filter) {
		return expirations.entrySet().stream().filter(entry -> entry.getValue().equals(filter)).map(Map.Entry::getKey)
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	@Nullable
	static Duration toDuration(@Nullable TimeToLive timeToLive) {

		if (timeToLive == null || timeToLive.sourceUnit == null) {
			return null;
		}

		return Duration.of(timeToLive.raw(), timeToLive.sourceUnit.toChronoUnit());
	}

	/**
	 * Collection of timeouts associated with a {@link TimeUnit}.
	 *
	 * @param timeUnit
	 * @param raw
	 */
	public record Timeouts(TimeUnit timeUnit, List<Long> raw) {

		Long get(int index) {
			return raw.get(index);
		}

		public int size() {
			return raw.size();
		}

	}

	/**
	 * Expiration holds time to live {@link #raw()} values as returned by a Redis Client. {@link #value()} serves the
	 * actual timeout in the given temporal context converting the {@link #raw()} value into a target {@link TimeUnit}.
	 * Dedicated methods such as {@link #isPersistent()} allow interpretation of the raw result. {@link #MISSING} and
	 * {@link #PERSISTENT} mark predefined states returned by Redis indicating a time to live value could not be retrieved
	 * due to various reasons.
	 */
	public static class TimeToLive {

		/**
		 * Predefined {@link TimeToLive} for a key that does not exist and therefore does not have a time to live.
		 */
		public static TimeToLive MISSING = new TimeToLive(-2L);

		/**
		 * Predefined {@link TimeToLive} for a key that exists but does not expire.
		 */
		public static TimeToLive PERSISTENT = new TimeToLive(-1L);

		private final @Nullable TimeUnit sourceUnit;
		private final @Nullable TimeUnit targetUnit;
		private final long raw;

		TimeToLive(long value) {
			this(value, null);
		}

		TimeToLive(long value, @Nullable TimeUnit sourceUnit) {
			this(value, sourceUnit, null);
		}

		TimeToLive(long value, @Nullable TimeUnit sourceUnit, @Nullable TimeUnit targetUnit) {
			this.raw = value;
			this.sourceUnit = sourceUnit;
			this.targetUnit = targetUnit;
		}

		/**
		 * Factory method for creating {@link TimeToLive} instances, returning predefined ones if the value matches a known
		 * reserved state.
		 *
		 * @param value the TTL value.
		 * @param timeUnit time unit for the given value.
		 * @return the {@link TimeToLive} for the given raw value.
		 */
		public static TimeToLive of(Number value, TimeUnit timeUnit) {

			return switch (value.intValue()) {
				case -2 -> MISSING;
				case -1 -> PERSISTENT;
				default -> new TimeToLive(value.longValue(), timeUnit);
			};
		}

		/**
		 * The raw source value as returned by the Redis Client.
		 *
		 * @return the raw data.
		 */
		public long raw() {
			return raw;
		}

		/**
		 * @return the {@link #raw()} value converted into the {@link #convert(TimeUnit) requested} target {@link TimeUnit}.
		 */
		public long value() {

			if (sourceUnit == null || targetUnit == null) {
				return raw;
			}

			return targetUnit.convert(raw, sourceUnit);
		}

		/**
		 * @param timeUnit must not be {@literal null}.
		 * @return the {@link TimeToLive} instance with new target {@link TimeUnit} set for obtaining the {@link #value()
		 *         value}, or the same instance raw value cannot or must not be converted.
		 */
		public TimeToLive convert(TimeUnit timeUnit) {

			if (sourceUnit == null || ObjectUtils.nullSafeEquals(sourceUnit, timeUnit)) {
				return this;
			}

			return new TimeToLive(raw, sourceUnit, timeUnit);
		}

		/**
		 * @return {@literal true} if key exists but does not expire.
		 */
		public boolean isPersistent() {
			return PERSISTENT.raw() == raw();
		}

		/**
		 * @return {@literal true} if key does not exist and therefore does not have a time to live.
		 */
		public boolean isMissing() {
			return MISSING.raw() == raw();
		}

		@Override
		public boolean equals(Object o) {

			if (o == this) {
				return true;
			}

			if (!(o instanceof Expirations.TimeToLive that)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(this.sourceUnit, that.sourceUnit)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(this.targetUnit, that.targetUnit)) {
				return false;
			}

			return this.raw == that.raw;
		}

		@Override
		public int hashCode() {
			return Objects.hash(raw);
		}

		@Override
		public String toString() {

			return switch ((int) raw()) {
				case -2 -> "MISSING";
				case -1 -> "PERSISTENT";
				default -> "%d %s".formatted(raw(), sourceUnit);
			};
		}
	}

}
