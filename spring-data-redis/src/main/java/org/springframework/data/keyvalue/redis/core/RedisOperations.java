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

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.keyvalue.redis.connection.DataType;
import org.springframework.data.keyvalue.redis.connection.SortParameters;


/**
 * Basic set of Redis operations, implemented by {@link RedisTemplate}. 
 * 
 * @author Costin Leau
 */
public interface RedisOperations<K, V> {

	Boolean exists(K key);

	void delete(Collection<K> key);

	DataType type(K key);

	Set<K> keys(K pattern);

	K randomKey();

	void rename(K oldKey, K newKey);

	Boolean renameIfAbsent(K oldKey, K newKey);

	Boolean expire(K key, long timeout, TimeUnit unit);

	Boolean expireAt(K key, Date date);

	void persist(K key);

	Long getExpire(K key);

	void watch(Collection<K> keys);

	void multi();

	Object exec();

	List<V> sort(K key, SortParameters params);

	Long sort(K key, SortParameters params, K destination);

	ValueOperations<K, V> valueOps();

	BoundValueOperations<K, V> forValue(K key);

	ListOperations<K, V> listOps();

	BoundListOperations<K, V> forList(K key);

	SetOperations<K, V> setOps();

	BoundSetOperations<K, V> forSet(K key);

	ZSetOperations<K, V> zSetOps();

	BoundZSetOperations<K, V> forZSet(K key);
	
	<HK, HV> HashOperations<K, HK, HV> hashOps();

	<HK, HV> BoundHashOperations<K, HK, HV> forHash(K key);
}
