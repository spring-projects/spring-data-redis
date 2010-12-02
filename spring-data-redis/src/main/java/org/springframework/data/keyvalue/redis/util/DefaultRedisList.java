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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.springframework.data.keyvalue.redis.core.BoundListOperations;
import org.springframework.data.keyvalue.redis.core.RedisOperations;

/**
 * Default implementation for {@link RedisList}. 
 * 
 * @author Costin Leau
 */
public class DefaultRedisList<E> extends AbstractRedisCollection<E> implements RedisList<E> {

	private final BoundListOperations<String, E> listOps;

	private class DefaultRedisListIterator<E> extends RedisIterator<E> {

		public DefaultRedisListIterator(Iterator<E> delegate) {
			super(delegate);
		}

		@Override
		protected void removeFromRedisStorage(E item) {
			DefaultRedisList.this.remove(item);
		}
	}

	/**
	 * Constructs a new <code>DefaultRedisList</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	public DefaultRedisList(String key, RedisOperations<String, E> operations) {
		super(key, operations);
		listOps = operations.forList(key);
	}

	public DefaultRedisList(BoundListOperations<String, E> boundOps) {
		super(boundOps.getKey(), boundOps.getOperations());
		listOps = boundOps;
	}

	@Override
	public List<E> range(long start, long end) {
		return listOps.range(start, end);
	}

	@Override
	public RedisList<E> trim(int start, int end) {
		listOps.trim(start, end);
		return this;
	}

	private List<E> content() {
		return listOps.range(0, -1);
	}

	@Override
	public Iterator<E> iterator() {
		return content().iterator();
	}

	@Override
	public int size() {
		return listOps.size().intValue();
	}


	@Override
	public boolean add(E value) {
		listOps.rightPush(value);
		return true;
	}

	@Override
	public void clear() {
		listOps.trim(size() + 1, 0);
	}

	@Override
	public boolean remove(Object o) {
		Long result = listOps.remove(0, o);
		return (result != null && result.longValue() > 0);
	}

	@Override
	public void add(int index, E element) {
		if (index == 0) {
			listOps.leftPush(element);
			return;
		}

		int size = size();

		if (index == size()) {
			listOps.rightPush(element);
			return;
		}

		if (index < 0 || index > size) {
			throw new IndexOutOfBoundsException();
		}

		throw new IllegalArgumentException("Redis supports insertion only at the beginning or the end of the list");
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		// insert collection in reverse
		if (index == 0) {
			Collection<? extends E> reverseC = CollectionUtils.reverse(c);

			for (E e : reverseC) {
				listOps.leftPush(e);
			}
			return true;
		}

		int size = size();

		if (index == size()) {
			for (E e : c) {
				listOps.rightPush(e);
			}
			return true;
		}

		if (index < 0 || index > size) {
			throw new IndexOutOfBoundsException();
		}

		throw new IllegalArgumentException("Redis supports insertion only at the beginning or the end of the list");
	}

	@Override
	public E get(int index) {
		if (index < 0 || index > size()) {
			throw new IndexOutOfBoundsException();
		}
		return listOps.index(index);
	}

	@Override
	public int indexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<E> listIterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public E remove(int index) {
		throw new UnsupportedOperationException();
	}


	@Override
	public E set(int index, E e) {
		E object = get(index);
		listOps.set(index, e);
		return object;
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}


	@Override
	public E element() {
		E value = peek();
		if (value == null)
			throw new NoSuchElementException();

		return value;
	}


	@Override
	public boolean offer(E e) {
		listOps.leftPush(e);
		return true;
	}


	@Override
	public E peek() {
		E element = listOps.index(0);
		return (element == null ? null : element);
	}


	@Override
	public E poll() {
		E element = listOps.leftPop();
		return (element == null ? null : element);
	}


	@Override
	public E remove() {
		E value = poll();
		if (value == null)
			throw new NoSuchElementException();

		return value;
	}
}