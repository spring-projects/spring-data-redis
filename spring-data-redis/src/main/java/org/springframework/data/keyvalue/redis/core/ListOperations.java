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
package org.springframework.data.keyvalue.redis.core;

import java.util.List;

/**
 * Redis, list specific operations.
 * 
 * @author Costin Leau
 */
public interface ListOperations<K, V> {

	List<V> range(K key, int start, int end);

	void trim(K key, int start, int end);

	Integer length(K key);

	Integer leftPush(K key, V value);

	Integer rightPush(K key, V value);

	void set(K key, int index, V value);

	Integer remove(K key, int i, Object value);

	V index(K key, int index);

	V leftPop(K key);

	V rightPop(K key);

	List<V> blockingLeftPop(int timeout, K... keys);

	List<V> blockingRightPop(int timeout, K... keys);

	RedisOperations<K, V> getOperations();
}
