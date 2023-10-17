/*
 * Copyright 2022-2023 the original author or authors.
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

/**
 * Value class representing a Redis Limit consisting of a maximum and an offset.
 *
 * @author Christoph Strobl
 * @since 1.6
 */
public class Limit {

	private static final Limit UNLIMITED = new Unlimited(0, 0);

	private final int offset;
	private final int count;

	Limit(int offset, int count) {
		this.offset = offset;
		this.count = count;
	}

	/**
	 * Create a new limit at {@code offset} zero.
	 *
	 * @return a new limit at {@code offset} zero.
	 */
	public static Limit limit() {
		return new Limit(0, 0);
	}

	/**
	 * Create a new limit at {@code offset} zero using the given {@code count}.
	 *
	 * @return a new limit at {@code offset} zero and the given {@code count}.
	 * @since 3.2
	 */
	public static Limit limit(int count) {
		return new Limit(0, count);
	}

	/**
	 * @return new {@link Limit} indicating no limit.
	 * @since 1.3
	 */
	public static Limit unlimited() {
		return UNLIMITED;
	}

	/**
	 * Create a new limit using the given {@code offset} retaining the configured count.
	 *
	 * @param offset the offset to use.
	 * @return a new limit using the given {@code offset} retaining the configured count.
	 */
	public Limit offset(int offset) {
		return new Limit(offset, this.count);
	}

	/**
	 * Create a new limit using the given {@code count} retaining the configured offset.
	 *
	 * @param count the count to use.
	 * @return a new limit using the given {@code offset} retaining the configured offset.
	 */
	public Limit count(int count) {
		return new Limit(this.offset, count);
	}

	public int getCount() {
		return count;
	}

	public int getOffset() {
		return offset;
	}

	/**
	 * @return {@code true} if this instance represents unlimited; {@code false} otherwise.
	 */
	public boolean isUnlimited() {
		return this.equals(UNLIMITED);
	}

	/**
	 * @return {@code true} if this instance represents a limit; {@code false} otherwise.
	 */
	public boolean isLimited() {
		return !isUnlimited();
	}

	private static class Unlimited extends Limit {

		private Unlimited(int offset, int count) {
			super(offset, count);
		}

		@Override
		public Limit offset(int offset) {
			throw new UnsupportedOperationException("Cannot set offset for Unlimited Limit");
		}

		@Override
		public int getCount() {
			return -1;
		}

		@Override
		public int getOffset() {
			return super.getOffset();
		}
	}
}
