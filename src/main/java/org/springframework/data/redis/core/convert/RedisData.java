/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Data object holding flat hash values, to be stored in Redis hash, representing the domain object. Index information
 * points to additional structures holding the objects is for searching.
 * 
 * @author Christoph Strobl
 */
public class RedisData {

	private String keyspace;
	private Serializable id;

	private Bucket bucket;
	private Set<IndexedData> indexedData;

	public RedisData() {
		this(Collections.<byte[], byte[]> emptyMap());
	}

	public RedisData(Map<byte[], byte[]> raw) {
		this(Bucket.newBucketFromRawMap(raw));
	}

	public RedisData(Bucket bucket) {

		this.bucket = bucket;
		this.indexedData = new HashSet<IndexedData>();
	}

	public void setId(Serializable id) {
		this.id = id;
	}

	public Serializable getId() {
		return this.id;
	}

	/**
	 * @param index
	 */
	public void addIndexedData(IndexedData index) {
		this.indexedData.add(index);
	}

	public Set<IndexedData> getIndexedData() {
		return Collections.unmodifiableSet(this.indexedData);
	}

	/**
	 * @return
	 */
	public String getKeyspace() {
		return keyspace;
	}

	/**
	 * @param keyspace
	 */
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public Bucket getBucket() {
		return bucket;
	};

	@Override
	public String toString() {
		return "RedisDataObject [key=" + keyspace + ":" + id + ", hash=" + bucket + "]";
	}

}
