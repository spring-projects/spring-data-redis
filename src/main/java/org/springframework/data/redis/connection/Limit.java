/*
 * Copyright 2022 the original author or authors.
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
 * @author Christoph Strobl
 * @since 1.6
 */
public class Limit {

	private static final Limit UNLIMITED = new Unlimited();

	int offset;
	int count;

	public static Limit limit() {
		return new Limit();
	}

	public Limit offset(int offset) {
		this.offset = offset;
		return this;
	}

	public Limit count(int count) {
		this.count = count;
		return this;
	}

	public int getCount() {
		return count;
	}

	public int getOffset() {
		return offset;
	}

	public boolean isUnlimited() {
		return this.equals(UNLIMITED);
	}

	public boolean isLimited() {
		return !isUnlimited();
	}

	/**
	 * @return new {@link Limit} indicating no limit;
	 * @since 1.3
	 */
	public static Limit unlimited() {
		return UNLIMITED;
	}

	private static class Unlimited extends Limit {

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
