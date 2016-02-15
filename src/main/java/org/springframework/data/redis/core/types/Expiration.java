/*
 * Copyright 2016 the original author or authors.
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

import java.util.concurrent.TimeUnit;

import org.springframework.util.Assert;

/**
 * Expiration holds a value with its associated {@link TimeUnit}.
 * 
 * @author Christoph Strobl
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
	protected Expiration(long expirationTime, TimeUnit timeUnit) {

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
	public boolean isPersitent() {
		return expirationTime == -1;
	}
}
