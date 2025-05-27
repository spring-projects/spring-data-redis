/*
 * Copyright 2011-2025 the original author or authors.
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

import java.util.Arrays;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;

/**
 * Default implementation of TypedTuple.
 *
 * @author Costin Leau
 */
public class DefaultTypedTuple<V> implements TypedTuple<V> {

	private final @Nullable Double score;
	private final @Nullable V value;

	/**
	 * Constructs a new <code>DefaultTypedTuple</code> instance.
	 *
	 * @param value can be {@literal null}.
	 * @param score can be {@literal null}.
	 */
	public DefaultTypedTuple(@Nullable V value, @Nullable Double score) {
		this.score = score;
		this.value = value;
	}

	public @Nullable Double getScore() {
		return score;
	}

	public @Nullable V getValue() {
		return value;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((score == null) ? 0 : score.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	public boolean equals(@Nullable Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof DefaultTypedTuple<?> other))
			return false;
		if (score == null) {
			if (other.score != null)
				return false;
		} else if (!score.equals(other.score))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (value instanceof byte[] bytes) {
			if (!(other.value instanceof byte[] otherBytes)) {
				return false;
			}
			return Arrays.equals(bytes, otherBytes);
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	public int compareTo(@Nullable Double o) {

		double thisScore = (score == null ? 0.0 : score);
		double otherScore = (o == null ? 0.0 : o);

		return Double.compare(thisScore, otherScore);
	}

	@Override
	public int compareTo(@Nullable TypedTuple<V> o) {

		if (o == null) {
			return compareTo(Double.valueOf(0));
		}

		return compareTo(o.getScore());
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append(getClass().getSimpleName());
		sb.append(" [score=").append(score);
		sb.append(", value=").append(value);
		sb.append(']');
		return sb.toString();
	}
}
