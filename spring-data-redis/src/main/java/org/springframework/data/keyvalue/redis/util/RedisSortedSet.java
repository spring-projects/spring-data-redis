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
package org.springframework.data.keyvalue.redis.util;

import java.util.Set;
import java.util.SortedSet;

/**
 * Redis extension for the {@link SortedSet} contract. Supports {@link SortedSet} specific
 * operations backed by Redis operations.
 * 
 * @author Costin Leau
 */
public interface RedisSortedSet<E> extends RedisStore<String>, SortedSet<E> {

	RedisSortedSet<E> intersectAndStore(String destKey, RedisSortedSet<E>... sets);

	RedisSortedSet<E> unionAndStore(String destKey, RedisSortedSet<E>... sets);

	Set<E> range(int start, int end);

	Set<E> rangeByScore(double min, double max);

	RedisSortedSet<E> remove(int start, int end);

	RedisSortedSet<E> removeByScore(double min, double max);
}
