/*
 * Copyright 2011-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.support.collections;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.RedisOperations;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Base implementation for {@link RedisCollection}. Provides a skeletal implementation. Note that the collection support
 * works only with normal, non-pipeline/multi-exec connections as it requires a reply to be sent right away.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public abstract class AbstractRedisCollection<E> extends AbstractCollection<E> implements RedisCollection<E> {

	public static final String ENCODING = "UTF-8";

	private volatile String key;

	private final RedisOperations<String, E> operations;

	/**
	 * Constructs a new {@link AbstractRedisCollection} instance.
	 *
	 * @param key Redis key of this collection.
	 * @param operations {@link RedisOperations} for the value type of this collection.
	 */
	public AbstractRedisCollection(String key, RedisOperations<String, E> operations) {

		Assert.hasText(key, "Key must not be empty!");
		Assert.notNull(operations, "RedisOperations must not be null!");

		this.key = key;
		this.operations = operations;
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public RedisOperations<String, E> getOperations() {
		return operations;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {

		boolean modified = false;

		for (E e : c) {
			modified |= add(e);
		}

		return modified;
	}

	@Override
	public boolean containsAll(Collection<?> c) {

		boolean contains = true;

		for (Object object : c) {
			contains &= contains(object);
		}

		return contains;
	}

	@Override
	public boolean removeAll(Collection<?> c) {

		boolean modified = false;

		for (Object object : c) {
			modified |= remove(object);
		}

		return modified;
	}

	@Override
	public Boolean expire(long timeout, TimeUnit unit) {
		return operations.expire(key, timeout, unit);
	}

	@Override
	public Boolean expireAt(Date date) {
		return operations.expireAt(key, date);
	}

	@Override
	public Long getExpire() {
		return operations.getExpire(key);
	}

	@Override
	public Boolean persist() {
		return operations.persist(key);
	}

	@Override
	public void rename(final String newKey) {

		if (!this.isEmpty()) {
			CollectionUtils.rename(key, newKey, operations);
		}

		key = newKey;
	}

	protected void checkResult(@Nullable Object obj) {

		if (obj == null) {
			throw new IllegalStateException("Cannot read collection with Redis connection in pipeline/multi-exec mode");
		}
	}

	@Override
	public boolean equals(Object o) {

		if (o == this)
			return true;

		if (o instanceof RedisStore) {
			return key.equals(((RedisStore) o).getKey());
		}

		if (o instanceof AbstractRedisCollection) {
			return o.hashCode() == hashCode();
		}

		return false;
	}

	@Override
	public int hashCode() {

		int result = 17 + getClass().hashCode();
		result = result * 31 + key.hashCode();

		return result;
	}

	@Override
	public String toString() {

		StringBuilder sb = new StringBuilder();

		sb.append(String.format("%s for key:", getClass().getSimpleName()));
		sb.append(getKey());

		return sb.toString();
	}

}
