/*
 * Copyright 2011-2018 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Redis extension for the {@link Set} contract. Supports {@link Set} specific operations backed by Redis operations.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 */
public interface RedisSet<E> extends RedisCollection<E>, Set<E> {

	Set<E> intersect(RedisSet<?> set);

	Set<E> intersect(Collection<? extends RedisSet<?>> sets);

	Set<E> union(RedisSet<?> set);

	Set<E> union(Collection<? extends RedisSet<?>> sets);

	Set<E> diff(RedisSet<?> set);

	Set<E> diff(Collection<? extends RedisSet<?>> sets);

	RedisSet<E> intersectAndStore(RedisSet<?> set, String destKey);

	RedisSet<E> intersectAndStore(Collection<? extends RedisSet<?>> sets, String destKey);

	RedisSet<E> unionAndStore(RedisSet<?> set, String destKey);

	RedisSet<E> unionAndStore(Collection<? extends RedisSet<?>> sets, String destKey);

	RedisSet<E> diffAndStore(RedisSet<?> set, String destKey);

	RedisSet<E> diffAndStore(Collection<? extends RedisSet<?>> sets, String destKey);

	/**
	 * @since 1.4
	 * @return
	 */
	Iterator<E> scan();
}
