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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

import org.springframework.datastore.redis.connection.RedisCommands;

/**
 * Default implementation for {@link RedisSortedSet}.
 * 
 * @author Costin Leau
 */
class DefaultRedisSortedSet extends AbstractRedisCollection implements RedisSortedSet {

	private class DefaultRedisSortedSetIterator extends RedisIterator {

		public DefaultRedisSortedSetIterator(Iterator<String> delegate) {
			super(delegate);
		}

		@Override
		protected void removeFromRedisStorage(String item) {
			DefaultRedisSortedSet.this.remove(item);
		}
	}

	public DefaultRedisSortedSet(String key, RedisCommands commands) {
		super(key, commands);
	}

	@Override
	public RedisSortedSet intersectAndStore(String destKey, RedisSet... sets) {
		return null;
	}

	@Override
	public List<String> range(int start, int end) {
		return null;
	}

	@Override
	public List<String> rangeByScore(int start, int end) {
		return null;
	}

	@Override
	public RedisSortedSet trim(int start, int end) {
		return null;
	}

	@Override
	public RedisSortedSet trimByScore(int start, int end) {
		return null;
	}

	@Override
	public RedisSortedSet unionAndStore(String destKey, RedisSet... sets) {
		return null;
	}

	@Override
	public String getKey() {
		return null;
	}

	@Override
	public boolean add(String e) {
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends String> c) {
		return false;
	}

	@Override
	public void clear() {
	}

	@Override
	public boolean contains(Object o) {
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return false;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public Iterator<String> iterator() {
		return null;
	}

	@Override
	public boolean remove(Object o) {
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return false;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public Object[] toArray() {
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return null;
	}

	@Override
	public Comparator<? super String> comparator() {
		return null;
	}

	@Override
	public String first() {
		return null;
	}

	@Override
	public SortedSet<String> headSet(String toElement) {
		return null;
	}

	@Override
	public String last() {
		return null;
	}

	@Override
	public SortedSet<String> subSet(String fromElement, String toElement) {
		return null;
	}

	@Override
	public SortedSet<String> tailSet(String fromElement) {
		return null;
	}
}