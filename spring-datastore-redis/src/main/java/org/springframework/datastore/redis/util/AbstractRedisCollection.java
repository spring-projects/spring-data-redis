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

import java.util.AbstractCollection;
import java.util.Collection;

import org.springframework.datastore.redis.connection.RedisCommands;

/**
 * Base implementation for Redis collections. 
 * 
 * @author Costin Leau
 */
public abstract class AbstractRedisCollection extends AbstractCollection<String> implements RedisStore {

	protected final String key;
	protected final RedisCommands commands;

	public AbstractRedisCollection(String key, RedisCommands commands) {
		this.key = key;
		this.commands = commands;
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public boolean addAll(Collection<? extends String> c) {
		boolean modified = false;
		for (String string : c) {
			modified |= add(string);
		}
		return modified;
	}

	public abstract boolean add(String e);

	public abstract void clear();

	@Override
	public boolean containsAll(Collection<?> c) {
		boolean contains = true;
		for (Object object : c) {
			contains &= contains(object);
		}
		return contains;
	}

	public abstract boolean remove(Object o);


	@Override
	public boolean removeAll(Collection<?> c) {
		boolean modified = false;
		for (Object object : c) {
			modified |= remove(object);
		}
		return modified;
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

}