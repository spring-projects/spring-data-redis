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
package org.springframework.data.keyvalue.redis.support.collections;

import java.util.Set;

/**
 * Redis extension for the {@link Set} contract. Supports {@link Set} specific
 * operations backed by Redis operations.
 * 
 * @author Costin Leau
 */
public interface RedisSet<E> extends RedisStore<String>, Set<E> {

	Set<E> intersect(RedisSet<? extends E>... sets);

	Set<E> union(RedisSet<? extends E>... sets);

	Set<E> diff(RedisSet<? extends E>... sets);

	RedisSet<E> intersectAndStore(String destKey, RedisSet<? extends E>... sets);

	RedisSet<E> unionAndStore(String destKey, RedisSet<? extends E>... sets);

	RedisSet<E> diffAndStore(String destKey, RedisSet<? extends E>... sets);
}
