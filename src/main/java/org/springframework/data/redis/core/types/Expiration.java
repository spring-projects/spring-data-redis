/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.core.types;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Expiration holds a value with its associated {@link TimeUnit}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class Expiration {

	private long expirationTime;
	private TimeUnit timeUnit;

	/**
	 * Creates new {@link Expiration}.
	 *
	 * @param expirationTime can be {@literal null}. Defaulted to {@link TimeUnit#SECONDS}
	 * @param timeUnit
	 */
	protected Expiration(long expirationTime, @Nullable TimeUnit timeUnit) {

		this.expirationTime = expirationTime;
		this.timeUnit = timeUnit != null ? timeUnit : TimeUnit.SECONDS;
	}

	/**
	 * Get the expiration time converted into {@link TimeUnit#MILLISECONDS}.
	 *
	 * @return
	 */
	public long getExpirationTimeInMilliseconds() {
		return getConverted(TimeUnit.MILLISECONDS);
	}

	/**
	 * Get the expiration time converted into {@link TimeUnit#SECONDS}.
	 *
	 * @return
	 */
	public long getExpirationTimeInSeconds() {
		return getConverted(TimeUnit.SECONDS);
	}

	/**
	 * Get the expiration time.
	 *
	 * @return
	 */
	public long getExpirationTime() {
		return expirationTime;
	}

	/**
	 * Get the time unit for the expiration time.
	 *
	 * @return
	 */
	public TimeUnit getTimeUnit() {
		return this.timeUnit;
	}

	/**
	 * Get the expiration time converted into the desired {@code targetTimeUnit}.
	 *
	 * @param targetTimeUnit must not {@literal null}.
	 * @return
	 * @throws IllegalArgumentException
	 */
	public long getConverted(TimeUnit targetTimeUnit) {

		Assert.notNull(targetTimeUnit, "TargetTimeUnit must not be null!");
		return targetTimeUnit.convert(expirationTime, timeUnit);
	}

	/**
	 * Creates new {@link Expiration} with {@link TimeUnit#SECONDS}.
	 *
	 * @param expirationTime
	 * @return
	 */
	public static Expiration seconds(long expirationTime) {
		return new Expiration(expirationTime, TimeUnit.SECONDS);
	}

	/**
	 * Creates new {@link Expiration} with {@link TimeUnit#MILLISECONDS}.
	 *
	 * @param expirationTime
	 * @return
	 */
	public static Expiration milliseconds(long expirationTime) {
		return new Expiration(expirationTime, TimeUnit.MILLISECONDS);
	}

	/**
	 * Creates new {@link Expiration} with the provided {@link TimeUnit}. Greater units than {@link TimeUnit#SECONDS} are
	 * converted to {@link TimeUnit#SECONDS}. Units smaller than {@link TimeUnit#MILLISECONDS} are converted to
	 * {@link TimeUnit#MILLISECONDS} and can lose precision since {@link TimeUnit#MILLISECONDS} is the smallest
	 * granularity supported by Redis.
	 *
	 * @param expirationTime
	 * @param timeUnit can be {@literal null}. Defaulted to {@link TimeUnit#SECONDS}
	 * @return
	 */
	public static Expiration from(long expirationTime, @Nullable TimeUnit timeUnit) {

		if (ObjectUtils.nullSafeEquals(timeUnit, TimeUnit.MICROSECONDS)
				|| ObjectUtils.nullSafeEquals(timeUnit, TimeUnit.NANOSECONDS)
				|| ObjectUtils.nullSafeEquals(timeUnit, TimeUnit.MILLISECONDS)) {
			return new Expiration(timeUnit.toMillis(expirationTime), TimeUnit.MILLISECONDS);
		}

		if (timeUnit != null) {
			return new Expiration(timeUnit.toSeconds(expirationTime), TimeUnit.SECONDS);
		}

		return new Expiration(expirationTime, TimeUnit.SECONDS);
	}

	/**
	 * Creates new {@link Expiration} with the provided {@link java.time.Duration}. Durations with at least
	 * {@link TimeUnit#SECONDS} resolution use seconds, durations using milliseconds use {@link TimeUnit#MILLISECONDS}
	 * resolution.
	 *
	 * @param duration must not be {@literal null}.
	 * @return
	 * @since 2.0
	 */
	public static Expiration from(Duration duration) {

		Assert.notNull(duration, "Duration must not be null!");

		if (duration.toMillis() % 1000 == 0) {
			return new Expiration(duration.getSeconds(), TimeUnit.SECONDS);
		}

		return new Expiration(duration.toMillis(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Creates new persistent {@link Expiration}.
	 *
	 * @return
	 */
	public static Expiration persistent() {
		return new Expiration(-1, TimeUnit.SECONDS);
	}

	/**
	 * @return {@literal true} if {@link Expiration} is set to persistent.
	 */
	public boolean isPersistent() {
		return expirationTime == -1;
	}
}
