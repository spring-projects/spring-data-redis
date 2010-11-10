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

import java.util.Iterator;
import java.util.Set;

import org.springframework.datastore.redis.connection.RedisCommands;

/**
 * Default implementation for {@link RedisSet}.
 * 
 * @author Costin Leau
 */
public class DefaultRedisSet extends AbstractRedisCollection<String> implements RedisSet {

	private class DefaultRedisSetIterator extends RedisIterator<String> {

		public DefaultRedisSetIterator(Iterator<String> delegate) {
			super(delegate);
		}

		@Override
		protected void removeFromRedisStorage(String item) {
			DefaultRedisSet.this.remove(item);
		}
	}

	/**
	 * Constructs a new <code>DefaultRedisSet</code> instance.
	 *
	 * @param key
	 * @param commands
	 */
	public DefaultRedisSet(String key, RedisCommands commands) {
		super(key, commands);
	}

	@Override
	public Set<String> diff(RedisSet... sets) {
		return commands.sDiff(extractKeys(sets));
	}

	@Override
	public RedisSet diffAndStore(String destKey, RedisSet... sets) {
		commands.sDiffStore(destKey, extractKeys(sets));
		return new DefaultRedisSet(destKey, commands);
	}

	@Override
	public Set<String> intersect(RedisSet... sets) {
		return commands.sInter(extractKeys(sets));
	}

	@Override
	public RedisSet intersectAndStore(String destKey, RedisSet... sets) {
		commands.sInterStore(destKey, extractKeys(sets));
		return new DefaultRedisSet(destKey, commands);
	}

	@Override
	public Set<String> union(RedisSet... sets) {
		return commands.sUnion(extractKeys(sets));
	}

	@Override
	public RedisSet unionAndStore(String destKey, RedisSet... sets) {
		commands.sUnionStore(destKey, extractKeys(sets));
		return new DefaultRedisSet(destKey, commands);
	}

	@Override
	public boolean add(String e) {
		return commands.sAdd(key, e);
	}

	@Override
	public void clear() {
		// intersect the set with a non existing one
		// TODO: find a safer way to clean the set
		commands.sInterStore(key, key, "NON-EXISTING");
	}

	@Override
	public boolean contains(Object o) {
		return commands.sIsMember(key, o.toString());
	}

	@Override
	public Iterator<String> iterator() {
		return new DefaultRedisSetIterator(commands.sMembers(key).iterator());
	}

	@Override
	public boolean remove(Object o) {
		return commands.sRem(key, o.toString());
	}

	@Override
	public int size() {
		return commands.sCard(key);
	}

	private String[] extractKeys(RedisSet... sets) {
		String[] keys = new String[sets.length + 1];
		keys[0] = key;
		for (int i = 0; i < keys.length; i++) {
			keys[i + 1] = sets[i].getKey();
		}

		return keys;
	}
}