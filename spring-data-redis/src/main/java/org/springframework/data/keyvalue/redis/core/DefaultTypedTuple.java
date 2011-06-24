/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.keyvalue.redis.core;

import org.springframework.data.keyvalue.redis.core.ZSetOperations.TypedTuple;

/**
 * Default implementation of TypedTuple.
 * 
 * @author Costin Leau
 */
class DefaultTypedTuple<V> implements TypedTuple<V> {

	private final Double score;
	private final V value;

	/**
	 * Constructs a new <code>DefaultTypedTuple</code> instance.
	 *
	 * @param value
	 * @param score
	 */
	public DefaultTypedTuple(V value, Double score) {
		this.score = score;
		this.value = value;
	}

	@Override
	public Double getScore() {
		return score;
	}

	@Override
	public V getValue() {
		return value;
	}
}
