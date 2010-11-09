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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
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
	public RedisSortedSet intersectAndStore(String destKey, RedisSortedSet... sets) {
		commands.zInterStore(destKey, extractKeys(sets));
		return new DefaultRedisSortedSet(destKey, commands);
	}

	@Override
	public Set<String> range(int start, int end) {
		return commands.zRange(key, start, end);
	}

	@Override
	public Set<String> rangeByScore(double min, double max) {
		return commands.zRangeByScore(key, min, max);
	}

	@Override
	public RedisSortedSet trim(int start, int end) {
		commands.zRemRange(key, start, end);
		return this;
	}

	@Override
	public RedisSortedSet trimByScore(double min, double max) {
		commands.zRemRangeByScore(key, min, max);
		return this;
	}

	@Override
	public RedisSortedSet unionAndStore(String destKey, RedisSortedSet... sets) {
		commands.zUnionStore(destKey, extractKeys(sets));
		return new DefaultRedisSortedSet(destKey, commands);
	}

	@Override
	public boolean add(String e) {
		return commands.zAdd(key, 0, e);
	}

	@Override
	public void clear() {
		commands.zRemRange(key, 0, -1);
	}

	@Override
	public boolean contains(Object o) {
		return (commands.zRank(key, o.toString()) != null);
	}

	@Override
	public Iterator<String> iterator() {
		return new DefaultRedisSortedSetIterator(commands.zRange(key, 0, -1).iterator());
	}

	@Override
	public boolean remove(Object o) {
		return commands.zRem(key, o.toString());
	}

	@Override
	public int size() {
		return commands.zCard(key);
	}

	@Override
	public Comparator<? super String> comparator() {
		return null;
	}

	@Override
	public String first() {
		return commands.zRange(key, 0, 0).iterator().next();
	}

	@Override
	public SortedSet<String> headSet(String toElement) {
		throw new UnsupportedOperationException();
	}

	@Override
	public String last() {
		return commands.zRevRange(key, 0, 0).iterator().next();
	}

	@Override
	public SortedSet<String> subSet(String fromElement, String toElement) {
		throw new UnsupportedOperationException();
	}

	@Override
	public SortedSet<String> tailSet(String fromElement) {
		throw new UnsupportedOperationException();
	}

	private String[] extractKeys(RedisSortedSet... sets) {
		String[] keys = new String[sets.length + 1];
		keys[0] = key;
		for (int i = 0; i < keys.length; i++) {
			keys[i + 1] = sets[i].getKey();
		}

		return keys;
	}
}