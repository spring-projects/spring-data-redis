/*
 * Copyright 2015-2018 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Data object holding {@link Bucket} representing the domain object to be stored in a Redis hash. Index information
 * points to additional structures holding the objects is for searching.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisData {

	private final Bucket bucket;
	private final Set<IndexedData> indexedData;

	private @Nullable String keyspace;
	private @Nullable String id;
	private @Nullable Long timeToLive;

	/**
	 * Creates new {@link RedisData} with empty {@link Bucket}.
	 */
	public RedisData() {
		this(Collections.emptyMap());
	}

	/**
	 * Creates new {@link RedisData} with {@link Bucket} holding provided values.
	 *
	 * @param raw should not be {@literal null}.
	 */
	public RedisData(Map<byte[], byte[]> raw) {
		this(Bucket.newBucketFromRawMap(raw));
	}

	/**
	 * Creates new {@link RedisData} with {@link Bucket}
	 *
	 * @param bucket must not be {@literal null}.
	 */
	public RedisData(Bucket bucket) {

		Assert.notNull(bucket, "Bucket must not be null!");

		this.bucket = bucket;
		this.indexedData = new HashSet<>();
	}

	/**
	 * Set the id to be used as part of the key.
	 *
	 * @param id
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return
	 */
	@Nullable
	public String getId() {
		return this.id;
	}

	/**
	 * Get the time before expiration in seconds.
	 *
	 * @return {@literal null} if not set.
	 */
	@Nullable
	public Long getTimeToLive() {
		return timeToLive;
	}

	/**
	 * @param index must not be {@literal null}.
	 */
	public void addIndexedData(IndexedData index) {

		Assert.notNull(index, "IndexedData to add must not be null!");
		this.indexedData.add(index);
	}

	/**
	 * @param indexes must not be {@literal null}.
	 */
	public void addIndexedData(Collection<IndexedData> indexes) {

		Assert.notNull(indexes, "IndexedData to add must not be null!");
		this.indexedData.addAll(indexes);
	}

	/**
	 * @return never {@literal null}.
	 */
	public Set<IndexedData> getIndexedData() {
		return Collections.unmodifiableSet(this.indexedData);
	}

	/**
	 * @return
	 */
	@Nullable
	public String getKeyspace() {
		return keyspace;
	}

	/**
	 * @param keyspace
	 */
	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	/**
	 * @return
	 */
	public Bucket getBucket() {
		return bucket;
	}

	/**
	 * Set the time before expiration in {@link TimeUnit#SECONDS}.
	 *
	 * @param timeToLive can be {@literal null}.
	 */
	public void setTimeToLive(Long timeToLive) {
		this.timeToLive = timeToLive;
	}

	/**
	 * Set the time before expiration converting the given arguments to {@link TimeUnit#SECONDS}.
	 *
	 * @param timeToLive must not be {@literal null}
	 * @param timeUnit must not be {@literal null}
	 */
	public void setTimeToLive(Long timeToLive, TimeUnit timeUnit) {

		Assert.notNull(timeToLive, "TimeToLive must not be null when used with TimeUnit!");
		Assert.notNull(timeToLive, "TimeUnit must not be null!");

		setTimeToLive(TimeUnit.SECONDS.convert(timeToLive, timeUnit));
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "RedisDataObject [key=" + keyspace + ":" + id + ", hash=" + bucket + "]";
	}

}
