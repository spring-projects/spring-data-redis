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
import java.util.Set;

import org.springframework.datastore.redis.connection.RedisCommands;

/**
 * Default implementation for {@link RedisSet}.
 * 
 * @author Costin Leau
 */
public class DefaultRedisSet extends AbstractRedisCollection implements RedisSet {

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
		return null;
	}

	@Override
	public RedisSet intersectAndStore(String destKey, RedisSet... sets) {
		return null;
	}

	@Override
	public Set<String> union(RedisSet... sets) {
		return null;
	}

	@Override
	public RedisSet unionAndStore(String destKey, RedisSet... sets) {
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
	public String element() {
		return null;
	}

	@Override
	public boolean offer(String e) {
		return false;
	}

	@Override
	public String peek() {
		return null;
	}

	@Override
	public String poll() {
		return null;
	}

	@Override
	public String remove() {
		return null;
	}

	private String[] extractKeys(RedisSet... sets) {
		String[] keys = new String[sets.length];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = sets[i].getKey();
		}

		return keys;
	}
}