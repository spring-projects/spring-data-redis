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
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.datastore.redis.connection.RedisCommands;

/**
 * Default {@link RedisMap} implementation.
 * 
 * @author Costin Leau
 */
public class DefaultRedisMap implements RedisMap {

	private class DefaultRedisMapEntry implements Map.Entry<String, String> {

		private String key, value;

		/**
		 * Constructs a new <code>DefaultRedisMapEntry</code> instance.
		 *
		 * @param entry
		 */
		public DefaultRedisMapEntry(org.springframework.datastore.redis.connection.RedisHashCommands.Entry entry) {
			this.key = entry.getField();
			this.value = entry.getValue();
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public String getValue() {
			return value;
		}

		@Override
		public String setValue(String value) {
			throw new UnsupportedOperationException();
		}
	}

	protected final String redisKey;
	protected final RedisCommands commands;

	/**
	 * Constructs a new <code>DefaultRedisMap</code> instance.
	 *
	 * @param key
	 * @param commands
	 */
	public DefaultRedisMap(String key, RedisCommands commands) {
		this.redisKey = key;
		this.commands = commands;
	}

	@Override
	public Integer increment(String key, int delta) {
		return commands.hIncrBy(redisKey, key, delta);
	}

	@Override
	public boolean putIfAbsent(String key, String value) {
		return commands.hSetNX(redisKey, key, value);
	}

	@Override
	public String getKey() {
		return redisKey;
	}

	@Override
	public RedisCommands getCommands() {
		return commands;
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsKey(Object key) {
		return commands.hExists(redisKey, key.toString());
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<java.util.Map.Entry<String, String>> entrySet() {
		return createEntrySet(commands.hGetAll(redisKey));
	}

	private Set<java.util.Map.Entry<String, String>> createEntrySet(Set<org.springframework.datastore.redis.connection.RedisHashCommands.Entry> entries) {
		Set<java.util.Map.Entry<String, String>> result = new LinkedHashSet<java.util.Map.Entry<String, String>>(
				entries.size());
		
		for (org.springframework.datastore.redis.connection.RedisHashCommands.Entry entry : entries) {
			result.add(new DefaultRedisMapEntry(entry));
		}
		return result;
	}

	@Override
	public String get(Object key) {
		return commands.hGet(redisKey, key.toString());
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public Set<String> keySet() {
		return commands.hKeys(redisKey);

	}

	@Override
	public String put(String key, String value) {
		String previous = commands.hGet(redisKey, key);
		if (commands.hSet(redisKey, key, value)) {
			return null;
		}
		return previous;
	}

	@Override
	public void putAll(Map<? extends String, ? extends String> m) {
		String[] keys = m.keySet().toArray(new String[m.size()]);
		String[] values = m.values().toArray(new String[m.size()]);

		commands.hMSet(redisKey, keys, values);
	}

	@Override
	public String remove(Object key) {
		String previous = commands.hGet(redisKey, key.toString());
		if (commands.hDel(redisKey, key.toString())) {
			return previous;
		}
		return null;
	}

	@Override
	public int size() {
		return commands.hLen(redisKey);
	}

	@Override
	public Collection<String> values() {
		return commands.hVals(redisKey);
	}
}