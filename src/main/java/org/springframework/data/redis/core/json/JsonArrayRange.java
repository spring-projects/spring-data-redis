/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.core.json;

import org.jspecify.annotations.NullMarked;

/**
 * Value object representing a range of array elements.
 *
 * @author Yordan Tsintsov
 * @since 4.3
 * @see <a href="https://redis.io/docs/latest/commands/json.arrindex/">Redis JSON.ARRINDEX</a>
 */
@NullMarked
public final class JsonArrayRange {

	private static final JsonArrayRange UNBOUNDED = new JsonArrayRange(0, 0);

	private final long start;
	private final long stop;

	private JsonArrayRange(long start, long stop) {
		this.start = start;
		this.stop = stop;
	}

	/**
	 * Create a new {@link JsonArrayRange} with the given start and stop indices.
	 *
	 * @param start the start index (inclusive)
	 * @param stop the stop index (exclusive). {@literal 0} indicates unbounded.
	 * @return a new {@link JsonArrayRange}
	 */
	public static JsonArrayRange of(long start, long stop) {

		if (start == 0 && stop == 0) {
			return UNBOUNDED;
		}
		return new JsonArrayRange(start, stop);
	}

	/**
	 * Create a new {@link JsonArrayRange} with no bounds.
	 *
	 * @return a new {@link JsonArrayRange}
	 */
	public static JsonArrayRange unbounded() {
		return UNBOUNDED;
	}

	/**
	 * Create a new {@link JsonArrayRange} with the given start index.
	 *
	 * @param start the start index (inclusive)
	 * @return a new {@link JsonArrayRange}
	 */
	public static JsonArrayRange from(long start) {

		if (start == 0) {
			return UNBOUNDED;
		}
		return new JsonArrayRange(start, 0);
	}

	/**
	 * Create a new {@link JsonArrayRange} with the given stop index.
	 *
	 * @param stop the stop index (exclusive). {@literal 0} indicates unbounded.
	 * @return a new {@link JsonArrayRange}
	 */
	public static JsonArrayRange to(long stop) {

		if (stop == 0) {
			return UNBOUNDED;
		}
		return new JsonArrayRange(0, stop);
	}

	public long getStart() {
		return start;
	}

	public long getStop() {
		return stop;
	}

	@Override
	public boolean equals(Object o) {

		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JsonArrayRange that = (JsonArrayRange) o;
		return start == that.start && stop == that.stop;
	}

	@Override
	public int hashCode() {
		return 31 * Long.hashCode(start) + Long.hashCode(stop);
	}

	@Override
	public String toString() {
		return "[" + start + ":" + stop + "]";
	}

}
