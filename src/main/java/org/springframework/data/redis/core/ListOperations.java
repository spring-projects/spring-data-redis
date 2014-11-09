/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Redis list specific operations.
 * 
 * @author Costin Leau
 * @author David Liu
 */
public interface ListOperations<K, V> {

	List<V> range(K key, long start, long end);

	void trim(K key, long start, long end);

	Long size(K key);

	Long leftPush(K key, V value);

	Long leftPushAll(K key, V... values);

	Long leftPushIfPresent(K key, V value);

	Long leftPush(K key, V pivot, V value);

	Long rightPush(K key, V value);

	Long rightPushAll(K key, V... values);

	Long rightPushIfPresent(K key, V value);

	Long rightPush(K key, V pivot, V value);

	void set(K key, long index, V value);

	Long remove(K key, long i, Object value);

	V index(K key, long index);

	V leftPop(K key);

	V leftPop(K key, long timeout, TimeUnit unit);

	V rightPop(K key);

	V rightPop(K key, long timeout, TimeUnit unit);

	V rightPopAndLeftPush(K sourceKey, K destinationKey);

	V rightPopAndLeftPush(K sourceKey, K destinationKey, long timeout, TimeUnit unit);

	RedisOperations<K, V> getOperations();
	
	Long rightPushAll(K key, Collection<V> values);
	
	Long leftPushAll(K key, Collection<V> values);
}
