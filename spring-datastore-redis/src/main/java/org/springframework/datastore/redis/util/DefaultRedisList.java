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
package org.springframework.datastore.redis.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

import org.springframework.datastore.redis.connection.RedisCommands;

/**
 * Default implementation for {@link RedisList}. 
 * 
 * @author Costin Leau
 */
public class DefaultRedisList<E> extends AbstractRedisCollection<E> implements RedisList<E> {

	private class DefaultRedisListIterator<E> extends RedisIterator<E> {

		public DefaultRedisListIterator(Iterator<E> delegate) {
			super(delegate);
		}

		@Override
		protected void removeFromRedisStorage(E item) {
			DefaultRedisList.this.remove(item);
		}
	}

	public DefaultRedisList(String key, RedisCommands commands) {
		super(key, commands);
	}

	@Override
	public List<E> range(int start, int end) {
		return CollectionUtils.deserializeAsList(commands.lRange(key, start, end), serializer);
	}

	@Override
	public RedisList<E> trim(int start, int end) {
		commands.lTrim(key, start, end);
		return this;
	}

	private List<E> content() {
		return CollectionUtils.deserializeAsList(commands.lRange(key, 0, -1), serializer);
	}

	@Override
	public Iterator<E> iterator() {
		return content().iterator();
	}

	@Override
	public int size() {
		return commands.lLen(key);
	}


	@Override
	public boolean add(E value) {
		commands.rPush(key, serializer.serializeAsString(value));
		return true;
	}

	@Override
	public void clear() {
		commands.lTrim(key, size() + 1, 0);
	}

	@Override
	public boolean remove(Object o) {
		Integer result = commands.lRem(key, 0, serializer.serializeAsString(o));
		return (result != null && result.intValue() > 0);
	}

	@Override
	public void add(int index, E element) {
		if (index == 0) {
			commands.lPush(key, serializer.serializeAsString(element));
			return;
		}

		int size = size();

		if (index == size()) {
			commands.rPush(key, serializer.serializeAsString(element));
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
				commands.lPush(key, serializer.serializeAsString(e));
			}
			return true;
		}

		int size = size();

		if (index == size()) {
			for (E e : c) {
				commands.rPush(key, serializer.serializeAsString(e));
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
		return serializer.deserialize(commands.lIndex(key, index));
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
		commands.lSet(key, index, serializer.serializeAsString(e));
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
		commands.lPush(key, serializer.serializeAsString(e));
		return true;
	}


	@Override
	public E peek() {
		return serializer.deserialize(commands.lIndex(key, 0));
	}


	@Override
	public E poll() {
		return serializer.deserialize(commands.lPop(key));
	}


	@Override
	public E remove() {
		E value = poll();
		if (value == null)
			throw new NoSuchElementException();

		return value;
	}
}