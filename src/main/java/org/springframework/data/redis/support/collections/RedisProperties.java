/*
 * Copyright 2011-2018 the original author or authors.
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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.lang.Nullable;

/**
 * {@link Properties} extension for a Redis back-store. Useful for reading (and storing) properties inside a Redis hash.
 * Particularly useful inside a Spring container for hooking into Spring's property placeholder or
 * {@link org.springframework.beans.factory.config.PropertiesFactoryBean}.
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
	 * Constructs a new {@link RedisProperties} instance.
	 */
	public RedisProperties(BoundHashOperations<String, String, String> boundOps) {
		this(null, boundOps);
	}

	/**
	 * Constructs a new {@link RedisProperties} instance.
	 *
	 * @param key Redis key of this property map.
	 * @param operations {@link RedisOperations} for this properties.
	 * @see RedisOperations#getHashKeySerializer()
	 * @see RedisOperations#getHashValueSerializer()
	 */
	public RedisProperties(String key, RedisOperations<String, ?> operations) {
		this(null, operations.<String, String> boundHashOps(key));
	}

	/**
	 * Constructs a new {@link RedisProperties} instance.
	 *
	 * @param defaults default properties to apply, can be {@literal null}.
	 * @param boundOps {@link BoundHashOperations} for this properties.
	 */
	public RedisProperties(@Nullable Properties defaults, BoundHashOperations<String, String, String> boundOps) {

		super(defaults);

		this.hashOps = boundOps;
		this.delegate = new DefaultRedisMap<>(boundOps);
	}

	/**
	 * Constructs a new {@link RedisProperties} instance.
	 *
	 * @param defaults default properties to apply, can be {@literal null}.
	 * @param key Redis key of this property map.
	 * @param operations {@link RedisOperations} for this properties.
	 * @see RedisOperations#getHashKeySerializer()
	 * @see RedisOperations#getHashValueSerializer()
	 */
	public RedisProperties(Properties defaults, String key, RedisOperations<String, ?> operations) {
		this(defaults, operations.boundHashOps(key));
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#get(java.lang.Object)
	 */
	@Override
	public synchronized Object get(Object key) {
		return delegate.get(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public synchronized Object put(Object key, Object value) {
		return delegate.put((String) key, (String) value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#putAll(java.util.Map)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public synchronized void putAll(Map<? extends Object, ? extends Object> t) {
		delegate.putAll((Map<? extends String, ? extends String>) t);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Properties#propertyNames()
	 */
	@Override
	public Enumeration<?> propertyNames() {
		Set<String> keys = new LinkedHashSet<>(delegate.keySet());
		if (defaults != null) {
			keys.addAll(defaults.stringPropertyNames());
		}
		return Collections.enumeration(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#clear()
	 */
	@Override
	public synchronized void clear() {
		delegate.clear();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#clone()
	 */
	@Override
	public synchronized Object clone() {
		return new RedisProperties(defaults, hashOps);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#contains(java.lang.Object)
	 */
	@Override
	public synchronized boolean contains(Object value) {
		return containsValue(value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#containsKey(java.lang.Object)
	 */
	@Override
	public synchronized boolean containsKey(Object key) {
		return delegate.containsKey(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(Object value) {
		return delegate.containsValue(value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#elements()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public synchronized Enumeration<Object> elements() {
		return Collections.enumeration((Collection) delegate.values());
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#entrySet()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Set<Entry<Object, Object>> entrySet() {
		return (Set) delegate.entrySet();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#equals(java.lang.Object)
	 */
	@Override
	public synchronized boolean equals(Object o) {

		if (o == this)
			return true;

		if (o instanceof RedisProperties) {
			return o.hashCode() == hashCode();
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#hashCode()
	 */
	@Override
	public synchronized int hashCode() {

		int hash = RedisProperties.class.hashCode();
		return hash * 17 + delegate.hashCode();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#isEmpty()
	 */
	@Override
	public synchronized boolean isEmpty() {
		return delegate.isEmpty();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#keys()
	 */
	@Override
	public synchronized Enumeration<Object> keys() {
		Set<Object> keys = keySet();
		return Collections.enumeration(keys);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#keySet()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Set<Object> keySet() {
		return (Set) delegate.keySet();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#remove(java.lang.Object)
	 */
	@Override
	public synchronized Object remove(Object key) {
		return delegate.remove(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#size()
	 */
	@Override
	public synchronized int size() {
		return delegate.size();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#values()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Collection<Object> values() {
		return (Collection) delegate.values();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisMap#increment(java.lang.Object, long)
	 */
	@Override
	public Long increment(Object key, long delta) {
		return hashOps.increment((String) key, delta);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisMap#increment(java.lang.Object, double)
	 */
	@Override
	public Double increment(Object key, double delta) {
		return hashOps.increment((String) key, delta);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisStore#getOperations()
	 */
	@Override
	public RedisOperations<String, ?> getOperations() {
		return hashOps.getOperations();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#expire(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public Boolean expire(long timeout, TimeUnit unit) {
		return hashOps.expire(timeout, unit);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#expireAt(java.util.Date)
	 */
	@Override
	public Boolean expireAt(Date date) {
		return hashOps.expireAt(date);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getExpire()
	 */
	@Override
	public Long getExpire() {
		return hashOps.getExpire();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getKey()
	 */
	@Override
	public String getKey() {
		return hashOps.getKey();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Override
	public DataType getType() {
		return hashOps.getType();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#persist()
	 */
	@Override
	public Boolean persist() {
		return hashOps.persist();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#rename(java.lang.Object)
	 */
	@Override
	public void rename(String newKey) {
		hashOps.rename(newKey);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#putIfAbsent(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Object putIfAbsent(Object key, Object value) {
		return (hashOps.putIfAbsent((String) key, (String) value) ? null : get(key));
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#remove(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean remove(Object key, Object value) {
		return delegate.remove(key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#replace(java.lang.Object, java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean replace(Object key, Object oldValue, Object newValue) {
		return delegate.replace((String) key, (String) oldValue, (String) newValue);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Hashtable#replace(java.lang.Object, java.lang.Object)
	 */
	@Override
	public Object replace(Object key, Object value) {
		return delegate.replace((String) key, (String) value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Properties#storeToXML(java.io.OutputStream, java.lang.String, java.lang.String)
	 */
	@Override
	public synchronized void storeToXML(OutputStream os, String comment, String encoding) throws IOException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Properties#storeToXML(java.io.OutputStream, java.lang.String)
	 */
	@Override
	public synchronized void storeToXML(OutputStream os, String comment) throws IOException {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.support.collections.RedisMap#scan()
	 */
	@Override
	public Iterator<java.util.Map.Entry<Object, Object>> scan() {
		throw new UnsupportedOperationException();
	}
}
