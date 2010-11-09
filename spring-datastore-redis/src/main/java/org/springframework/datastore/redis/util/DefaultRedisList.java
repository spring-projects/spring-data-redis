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
public class DefaultRedisList extends AbstractRedisCollection implements RedisList {

	private class DefaultRedisListIterator extends RedisIterator {

		public DefaultRedisListIterator(Iterator<String> delegate) {
			super(delegate);
		}

		@Override
		protected void removeFromRedisStorage(String item) {
			DefaultRedisList.this.remove(item);
		}
	}

	public DefaultRedisList(String key, RedisCommands commands) {
		super(key, commands);
	}

	@Override
	public List<String> range(int start, int end) {
		return commands.lRange(key, start, end);
	}

	@Override
	public RedisList trim(int start, int end) {
		commands.lTrim(key, start, end);
		return this;
	}

	private List<String> content() {
		return commands.lRange(key, 0, -1);
	}

	@Override
	public Iterator<String> iterator() {
		return content().iterator();
	}

	@Override
	public int size() {
		return commands.lLen(key);
	}


	@Override
	public boolean add(String value) {
		commands.rPush(key, value);
		return true;
	}

	@Override
	public void clear() {
		commands.lTrim(key, 0, -1);
	}

	@Override
	public boolean remove(Object o) {
		Integer result = commands.lRem(key, 0, o.toString());
		return (result != null && result.intValue() > 0);
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
	public void add(int index, String element) {
		if (index == 0) {
			commands.lPush(key, element);
		}
		else if (index == size()) {
			commands.rPush(key, element);
		}

		throw new IllegalArgumentException("Redis supports insertion only at the beginning or the end of the list");
	}

	@Override
	public boolean addAll(int index, Collection<? extends String> c) {
		for (String string : c) {
			add(index, string);
		}

		return true;
	}

	@Override
	public String get(int index) {
		return commands.lIndex(key, index);
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
	public ListIterator<String> listIterator() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ListIterator<String> listIterator(int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String remove(int index) {
		throw new UnsupportedOperationException();
	}


	@Override
	public String set(int index, String element) {
		String object = get(index);
		commands.lSet(key, index, element);
		return object;
	}

	@Override
	public List<String> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}


	@Override
	public String element() {
		String value = peek();
		if (value == null)
			throw new NoSuchElementException();

		return value;
	}


	@Override
	public boolean offer(String e) {
		commands.lPush(key, e);
		return true;
	}


	@Override
	public String peek() {
		return commands.lIndex(key, 0);
	}


	@Override
	public String poll() {
		return commands.lPop(key);
	}


	@Override
	public String remove() {
		String value = poll();
		if (value == null)
			throw new NoSuchElementException();

		return value;
	}
}