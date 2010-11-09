/*
 * Copyright 2010 the original author or authors.
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
package org.springframework.datastore.redis.connection;

import org.springframework.datastore.redis.connection.RedisZSetCommands.Tuple;

/**
 * Default implementation for {@link Tuple} interface.
 * 
 * @author Costin Leau
 */
public class DefaultTuple implements Tuple {

	private final Double score;
	private final String value;


	/**
	 * Constructs a new <code>DefaultTuple</code> instance.
	 *
	 * @param value
	 * @param score
	 */
	public DefaultTuple(String value, Double score) {
		this.score = score;
		this.value = value;
	}

	@Override
	public Double getScore() {
		return score;
	}

	@Override
	public String getValue() {
		return value;
	}
}
