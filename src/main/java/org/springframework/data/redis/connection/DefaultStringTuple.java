/*
 * Copyright 2011-2022 the original author or authors.
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

import java.nio.charset.StandardCharsets;

import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;
import org.springframework.data.redis.connection.zset.DefaultTuple;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Default implementation for {@link StringTuple} interface.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class DefaultStringTuple extends DefaultTuple implements StringTuple {

	private final String valueAsString;

	/**
	 * Constructs a new <code>DefaultStringTuple</code> instance.
	 *
	 * @param value
	 * @param score
	 */
	public DefaultStringTuple(byte[] value, String valueAsString, Double score) {

		super(value, score);
		this.valueAsString = valueAsString;

	}

	/**
	 * Constructs a new <code>DefaultStringTuple</code> instance.
	 *
	 * @param valueAsString must not be {@literal null}.
	 * @param score
	 * @since 2.6
	 */
	public DefaultStringTuple(String valueAsString, double score) {
		this(valueAsString.getBytes(StandardCharsets.UTF_8), valueAsString, score);
	}

	/**
	 * Constructs a new <code>DefaultStringTuple</code> instance.
	 *
	 * @param tuple
	 * @param valueAsString
	 */
	public DefaultStringTuple(Tuple tuple, String valueAsString) {

		super(tuple.getValue(), tuple.getScore());
		this.valueAsString = valueAsString;
	}

	public String getValueAsString() {
		return valueAsString;
	}

	public String toString() {
		return "DefaultStringTuple[value=" + getValueAsString() + ", score=" + getScore() + "]";
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		if (!super.equals(o))
			return false;

		DefaultStringTuple that = (DefaultStringTuple) o;

		return ObjectUtils.nullSafeEquals(valueAsString, that.valueAsString);
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + ObjectUtils.nullSafeHashCode(valueAsString);
		return result;
	}
}
