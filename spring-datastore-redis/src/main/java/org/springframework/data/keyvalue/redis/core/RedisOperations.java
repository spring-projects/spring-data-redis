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


/**
 * Basic set of Redis operations, implemented by {@link RedisTemplate}. 
 * 
 * @author Costin Leau
 */
public interface RedisOperations<K, V> {

	void set(K key, V value);

	V get(K key);

	V getAndSet(K key, V newValue);

	void watch(K... keys);

	void multi();

	Object exec();

	Integer increment(K key, int delta);

	void delete(K... keys);

	ListOperations<K, V> listOps();

	BoundListOperations<K, V> forList(K key);

	SetOperations<K, V> setOps();

	BoundSetOperations<K, V> forSet(K key);

	ZSetOperations<K, V> zSetOps();

	BoundZSetOperations<K, V> forZSet(K key);
}
