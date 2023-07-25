/*
 * Copyright 2016-2023 the original author or authors.
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
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * {@link Expiration} holds a {@link Long numeric value} with an associated {@link TimeUnit}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 * @see java.time.Duration
 * @see java.util.concurrent.TimeUnit
 * @since 1.7
 */
public class Expiration {

	/**
	 * Creates a new {@link Expiration} in {@link TimeUnit#MILLISECONDS}.
	 *
	 * @param expirationTime {@link Long length of time} for expiration.
	 * @return a new {@link Expiration} measured in {@link TimeUnit#MILLISECONDS}.
	 */
	public static Expiration milliseconds(long expirationTime) {
		return new Expiration(expirationTime, TimeUnit.MILLISECONDS);
	}

	/**
	 * Creates a new {@link Expiration} in {@link TimeUnit#SECONDS}.
	 *
	 * @param expirationTime {@link Long length of time} for expiration.
	 * @return a new {@link Expiration} measured in {@link TimeUnit#SECONDS}.
	 */
	public static Expiration seconds(long expirationTime) {
		return new Expiration(expirationTime, TimeUnit.SECONDS);
	}

	/**
	 * Creates a new {@link Expiration} with the given {@literal unix timestamp} and {@link TimeUnit}.
	 *
	 * @param unixTimestamp {@link Long unix timestamp} at which the key will expire.
	 * @param timeUnit {@link TimeUnit} used to measure the expiration period; must not be {@literal null}.
	 * @return a new {@link Expiration} with the given {@literal unix timestamp} and {@link TimeUnit}.
	 */
	public static Expiration unixTimestamp(long unixTimestamp, TimeUnit timeUnit) {
		return new ExpireAt(unixTimestamp, timeUnit);
	}

	/**
	 * Creates new {@link Expiration} with the provided {@link TimeUnit}. Greater units than {@link TimeUnit#SECONDS} are
	 * converted to {@link TimeUnit#SECONDS}. Units smaller than {@link TimeUnit#MILLISECONDS} are converted to
	 * {@link TimeUnit#MILLISECONDS} and can lose precision since {@link TimeUnit#MILLISECONDS} is the smallest
	 * granularity supported by Redis.
	 *
	 * @param expirationTime {@link Long length of time} for the {@link Expiration}.
	 * @param timeUnit {@link TimeUnit} used to measure the {@link Long expiration time}; can be {@literal null}.
	 * Defaulted to {@link TimeUnit#SECONDS}
	 * @return a new {@link Expiration} configured with the given {@link Long length of time} in {@link TimeUnit}.
	 */
	public static Expiration from(long expirationTime, @Nullable TimeUnit timeUnit) {

		if (TimeUnit.NANOSECONDS.equals(timeUnit)
			|| TimeUnit.MICROSECONDS.equals(timeUnit)
			|| TimeUnit.MILLISECONDS.equals(timeUnit)) {

			return new Expiration(timeUnit.toMillis(expirationTime), TimeUnit.MILLISECONDS);
		}

		return timeUnit != null ? new Expiration(timeUnit.toSeconds(expirationTime), TimeUnit.SECONDS)
			: new Expiration(expirationTime, TimeUnit.SECONDS);
	}

	/**
	 * Creates a new {@link Expiration} with the given, required {@link Duration}.
	 * <p>
	 * Durations with at least {@literal seconds} resolution uses {@link TimeUnit#SECONDS}. {@link Duration Durations}
	 * in {@literal milliseconds} use {@link TimeUnit#MILLISECONDS}.
	 *
	 * @param duration must not be {@literal null}.
	 * @return a new {@link Expiration} from the given {@link Duration}.
	 * @since 2.0
	 */
	public static Expiration from(Duration duration) {

		Assert.notNull(duration, "Duration must not be null");

		return duration.isZero() ? Expiration.persistent()
			: duration.toMillis() % 1000 == 0 ? new Expiration(duration.getSeconds(), TimeUnit.SECONDS)
			: new Expiration(duration.toMillis(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Obtain an {@link Expiration} that indicates to keep the existing one, e.g. when sending a {@code SET} command.
	 * <p>
	 * <strong>NOTE: </strong>Please follow the documentation for the individual commands to see
	 * if keeping the existing TTL is applicable.
	 *
	 * @return never {@literal null}.
	 * @since 2.4
	 */
	public static Expiration keepTtl() {
		return KeepTtl.INSTANCE;
	}

	/**
	 * Creates a new persistent, non-expiring {@link Expiration}.
	 *
	 * @return a new persistent, non-expiring {@link Expiration}.
	 */
	public static Expiration persistent() {
		return new Expiration(-1, TimeUnit.SECONDS);
	}

	private final long expirationTime;

	private final TimeUnit timeUnit;

	/**
	 * Creates new {@link Expiration}.
	 *
	 * @param expirationTime {@link Long length of time} for expiration. Defaulted to {@link TimeUnit#SECONDS}.
	 * @param timeUnit {@link TimeUnit} used to measure {@link Long expirationTime}.
	 */
	protected Expiration(long expirationTime, @Nullable TimeUnit timeUnit) {

		this.expirationTime = expirationTime;
		this.timeUnit = timeUnit != null ? timeUnit : TimeUnit.SECONDS;
	}

	/**
	 * Get the {@link Long length of time} for this {@link Expiration}.
	 *
	 * @return the {@link Long length of time} for this {@link Expiration}.
	 */
	public long getExpirationTime() {
		return this.expirationTime;
	}

	/**
	 * Get the {@link Long expiration time} converted into {@link TimeUnit#MILLISECONDS}.
	 *
	 * @return the expiration time converted into {@link TimeUnit#MILLISECONDS}.
	 */
	public long getExpirationTimeInMilliseconds() {
		return getConverted(TimeUnit.MILLISECONDS);
	}

	/**
	 * Get the {@link Long expiration time} converted into {@link TimeUnit#SECONDS}.
	 *
	 * @return the {@link Long expiration time} converted into {@link TimeUnit#SECONDS}.
	 */
	public long getExpirationTimeInSeconds() {
		return getConverted(TimeUnit.SECONDS);
	}

	/**
	 * Get the configured {@link TimeUnit} for the {@link #getExpirationTime() expiration time}.
	 *
	 * @return the configured {@link TimeUnit} for the {@link #getExpirationTime() expiration time}.
	 */
	public TimeUnit getTimeUnit() {
		return this.timeUnit;
	}

	/**
	 * Converts {@link #getExpirationTime() expiration time} into the given, desired {@link TimeUnit}.
	 *
	 * @param targetTimeUnit {@link TimeUnit} used to convert the {@link #getExpirationTime()} expiration time};
	 * must not be {@literal null}.
	 * @return the {@link #getExpirationTime() expiration time} converted into the given, desired {@link TimeUnit}.
	 * @throws IllegalArgumentException if the given {@link TimeUnit} is {@literal null}.
	 */
	public long getConverted(TimeUnit targetTimeUnit) {

		Assert.notNull(targetTimeUnit, "TimeUnit must not be null");

		return targetTimeUnit.convert(getExpirationTime(), getTimeUnit());
	}

	/**
	 * @return {@literal true} if {@link Expiration} is set to persistent.
	 */
	public boolean isPersistent() {
		return getExpirationTime() == -1;
	}

	/**
	 * @return {@literal true} if {@link Expiration} of existing key should not be modified.
	 * @since 2.4
	 */
	public boolean isKeepTtl() {
		return false;
	}

	/**
	 * @return {@literal true} if {@link Expiration} is set to a specified Unix time at which the key will expire.
	 * @since 2.6
	 */
	public boolean isUnixTimestamp() {
		return false;
	}

	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (!(obj instanceof Expiration that)) {
			return false;
		}

		return this.getTimeUnit().toMillis(getExpirationTime()) == that.getTimeUnit().toMillis(that.getExpirationTime());
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(new Object[] { getExpirationTime(), getTimeUnit() });
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.4
	 */
	private static class KeepTtl extends Expiration {

		static KeepTtl INSTANCE = new KeepTtl();

		private KeepTtl() {
			super(-2, null);
		}

		@Override
		public boolean isKeepTtl() {
			return true;
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.6
	 */
	private static class ExpireAt extends Expiration {

		private ExpireAt(long expirationTime, @Nullable TimeUnit timeUnit) {
			super(expirationTime, timeUnit);
		}

		public boolean isUnixTimestamp() {
			return true;
		}
	}
}
