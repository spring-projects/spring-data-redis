/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisOperations;

/**
 * {@link Properties} extension for a Redis back-store. Useful for reading (and storing) properties
 * inside a Redis hash. Particularly useful inside a Spring container for hooking into Spring's property
 * placeholder or {@link org.springframework.beans.factory.config.PropertiesFactoryBean}.
 * <p/>
 * Note that this implementation only accepts Strings - objects of other type are not supported.
 * 
 * @see Properties
 * @see org.springframework.core.io.support.PropertiesLoaderSupport
 * @author Costin Leau
 */
public class RedisProperties extends Properties implements RedisMap<Object, Object> {

	private final BoundHashOperations<String, String, String> hashOps;
	private final RedisMap<String, String> delegate;

	/**
	 * Constructs a new <code>RedisProperties</code> instance.
	 *
	 */
	public RedisProperties(BoundHashOperations<String, String, String> boundOps) {
		this(null, boundOps);
	}

	/**
	 * Constructs a new <code>RedisProperties</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	public RedisProperties(String key, RedisOperations<String, ?> operations) {
		this(null, operations.<String, String> boundHashOps(key));
	}

	/**
	 * Constructs a new <code>RedisProperties</code> instance.
	 *
	 * @param defaults
	 * @param boundOps
	 */
	public RedisProperties(Properties defaults, BoundHashOperations<String, String, String> boundOps) {
		super(defaults);
		this.hashOps = boundOps;
		this.delegate = new DefaultRedisMap<String, String>(boundOps);
	}

	/**
	 * Constructs a new <code>RedisProperties</code> instance.
	 *
	 * @param defaults
	 * @param key
	 * @param operations
	 */
	public RedisProperties(Properties defaults, String key, RedisOperations<String, ?> operations) {
		this(defaults, operations.<String, String> boundHashOps(key));
	}

	
	public synchronized Object get(Object key) {
		return delegate.get(key);
	}

	
	public synchronized Object put(Object key, Object value) {
		return delegate.put((String) key, (String) value);
	}

	@SuppressWarnings("unchecked")
	
	public synchronized void putAll(Map<? extends Object, ? extends Object> t) {
		delegate.putAll((Map<? extends String, ? extends String>) t);
	}

	
	public Enumeration<?> propertyNames() {
		Set<String> keys = new LinkedHashSet<String>(delegate.keySet());
		if (defaults != null) {
			keys.addAll(defaults.stringPropertyNames());
		}
		return Collections.enumeration(keys);
	}

	
	public synchronized void clear() {
		delegate.clear();
	}

	
	public synchronized Object clone() {
		return new RedisProperties(defaults, hashOps);
	}

	
	public synchronized boolean contains(Object value) {
		return containsValue(value);
	}

	
	public synchronized boolean containsKey(Object key) {
		return delegate.containsKey(key);
	}

	
	public boolean containsValue(Object value) {
		return delegate.containsValue(value);
	}

	@SuppressWarnings("unchecked")
	
	public synchronized Enumeration<Object> elements() {
		Collection values = delegate.values();
		return Collections.enumeration(values);
	}

	
	@SuppressWarnings("unchecked")
	public Set<Entry<Object, Object>> entrySet() {
		Set entries = delegate.entrySet();
		return entries;
	}

	
	public synchronized boolean equals(Object o) {
		if (o == this)
			return true;

		if (o instanceof RedisProperties) {
			return o.hashCode() == hashCode();
		}
		return false;
	}

	
	public synchronized int hashCode() {
		int hash = RedisProperties.class.hashCode();
		return hash * 17 + delegate.hashCode();
	}

	
	public synchronized boolean isEmpty() {
		return delegate.isEmpty();
	}

	
	public synchronized Enumeration<Object> keys() {
		Set<Object> keys = keySet();
		return Collections.enumeration(keys);
	}

	@SuppressWarnings("unchecked")
	
	public Set<Object> keySet() {
		Set keys = delegate.keySet();
		return keys;
	}

	
	public synchronized Object remove(Object key) {
		return delegate.remove(key);
	}

	
	public synchronized int size() {
		return delegate.size();
	}

	@SuppressWarnings("unchecked")
	
	public Collection<Object> values() {
		Collection vals = delegate.values();
		return vals;
	}

	
	public Long increment(Object key, long delta) {
		return hashOps.increment((String) key, delta);
	}
	
	public Double increment(Object key, double delta) {
		return hashOps.increment((String) key, delta);
	}

	public RedisOperations<String, ?> getOperations() {
		return hashOps.getOperations();
	}

	
	public Boolean expire(long timeout, TimeUnit unit) {
		return hashOps.expire(timeout, unit);
	}

	
	public Boolean expireAt(Date date) {
		return hashOps.expireAt(date);
	}

	
	public Long getExpire() {
		return hashOps.getExpire();
	}

	
	public String getKey() {
		return hashOps.getKey();
	}

	
	public DataType getType() {
		return hashOps.getType();
	}

	
	public Boolean persist() {
		return hashOps.persist();
	}

	
	public void rename(String newKey) {
		hashOps.rename(newKey);
	}

	
	public Object putIfAbsent(Object key, Object value) {
		return (hashOps.putIfAbsent((String) key, (String) value) ? null : get(key));
	}

	
	public boolean remove(Object key, Object value) {
		return delegate.remove(key, value);
	}

	
	public boolean replace(Object key, Object oldValue, Object newValue) {
		return delegate.replace((String) key, (String) oldValue, (String) newValue);
	}

	
	public Object replace(Object key, Object value) {
		return delegate.replace((String) key, (String) value);
	}

	
	public synchronized void storeToXML(OutputStream os, String comment, String encoding) throws IOException {
		throw new UnsupportedOperationException();
	}

	
	public synchronized void storeToXML(OutputStream os, String comment) throws IOException {
		throw new UnsupportedOperationException();
	}
}