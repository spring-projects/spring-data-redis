/*
 * Copyright 2011-2025 the original author or authors.
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

import java.util.Iterator;

import org.jspecify.annotations.Nullable;

/**
 * Iterator extension for Redis collection removal.
 *
 * @param <E> the type of elements in this collection.
 * @author Costin Leau
 */
abstract class RedisIterator<E> implements Iterator<E> {

	private final Iterator<E> delegate;

	private @Nullable E item;

	/**
	 * Constructs a new <code>RedisIterator</code> instance.
	 *
	 * @param delegate
	 */
	RedisIterator(Iterator<E> delegate) {
		this.delegate = delegate;
	}

	/**
	 * @return
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return delegate.hasNext();
	}

	/**
	 * @return
	 * @see java.util.Iterator#next()
	 */
	@Override
	public E next() {
		item = delegate.next();
		return item;
	}

	/**
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		delegate.remove();
		removeFromRedisStorage(item);
		item = null;
	}

	protected abstract void removeFromRedisStorage(E item);
}
