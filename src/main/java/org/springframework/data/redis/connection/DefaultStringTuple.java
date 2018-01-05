/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.connection;

import lombok.EqualsAndHashCode;

import org.springframework.data.redis.connection.RedisZSetCommands.Tuple;
import org.springframework.data.redis.connection.StringRedisConnection.StringTuple;

/**
 * Default implementation for {@link StringTuple} interface.
 *
 * @author Costin Leau
 */
@EqualsAndHashCode(callSuper = true)
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
}
