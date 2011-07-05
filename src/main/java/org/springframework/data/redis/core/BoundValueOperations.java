/*
 * Copyright 2010-2011 the original author or authors.
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

import java.util.concurrent.TimeUnit;

/**
 * Value (or String in Redis terminology) operations bound to a certain key. 
 * 
 * @author Costin Leau
 */
public interface BoundValueOperations<K, V> extends BoundKeyOperations<K> {

	RedisOperations<K, V> getOperations();

	void set(V value);

	void set(V value, long offset);

	void set(V value, long timeout, TimeUnit unit);

	Boolean setIfAbsent(V value);

	V get();

	String get(long start, long end);

	V getAndSet(V value);

	Long increment(long delta);

	Integer append(String value);

	Long size();
}
