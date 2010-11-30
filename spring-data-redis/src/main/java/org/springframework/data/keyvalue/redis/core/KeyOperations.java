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

import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.keyvalue.redis.connection.DataType;

/**
 * Redis operations available for all keys.
 * 
 * @author Costin Leau
 */
public interface KeyOperations<K> {

	Boolean exists(K key);

	void delete(K key);

	DataType type(K key);

	Set<K> keys(String pattern);

	K randomKey();

	void rename(K oldKey, K newKey);

	Boolean renameIfAbsent(K oldKey, K newKey);

	Boolean expire(K key, long timeout, TimeUnit unit);

	Boolean expireAt(K key, Date date);

	void persist(K key);

	long getExpire(K key);
}