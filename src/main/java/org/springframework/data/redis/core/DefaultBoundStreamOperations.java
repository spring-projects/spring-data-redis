/*
 * Copyright 2018-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.core;

import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.lang.Nullable;

/**
 * Default implementation for {@link BoundStreamOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
class DefaultBoundStreamOperations<K, HK, HV> extends DefaultBoundKeyOperations<K>
		implements BoundStreamOperations<K, HK, HV> {

	private final StreamOperations<K, HK, HV> ops;

	/**
	 * Constructs a new <code>DefaultBoundSetOperations</code> instance.
	 *
	 * @param key
	 * @param operations
	 */
	DefaultBoundStreamOperations(K key, RedisOperations<K, ?> operations) {

		super(key, operations);
		this.ops = operations.opsForStream();
	}

	@Nullable
	@Override
	public Long acknowledge(String group, String... recordIds) {
		return ops.acknowledge(getKey(), group, recordIds);
	}

	@Nullable
	@Override
	public RecordId add(Map<HK, HV> body) {
		return ops.add(getKey(), body);
	}

	@Nullable
	@Override
	public Long delete(String... recordIds) {
		return ops.delete(getKey(), recordIds);
	}

	@Nullable
	@Override
	public String createGroup(ReadOffset readOffset, String group) {
		return ops.createGroup(getKey(), readOffset, group);
	}

	@Nullable
	@Override
	public Boolean deleteConsumer(Consumer consumer) {
		return ops.deleteConsumer(getKey(), consumer);
	}

	@Nullable
	@Override
	public Boolean destroyGroup(String group) {
		return ops.destroyGroup(getKey(), group);
	}

	@Nullable
	@Override
	public Long size() {
		return ops.size(getKey());
	}

	@Nullable
	@Override
	public List<MapRecord<K, HK, HV>> range(Range<String> range, Limit limit) {
		return ops.range(getKey(), range, limit);
	}

	@Nullable
	@Override
	public List<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, ReadOffset readOffset) {
		return ops.read(readOptions, StreamOffset.create(getKey(), readOffset));
	}

	@Nullable
	@Override
	public List<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, ReadOffset readOffset) {
		return ops.read(consumer, readOptions, StreamOffset.create(getKey(), readOffset));
	}

	@Nullable
	@Override
	public List<MapRecord<K, HK, HV>> reverseRange(Range<String> range, Limit limit) {
		return ops.reverseRange(getKey(), range, limit);
	}

	@Nullable
	@Override
	public Long trim(long count) {
		return trim(count, false);
	}

	@Nullable
	@Override
	public Long trim(long count, boolean approximateTrimming) {
		return ops.trim(getKey(), count, approximateTrimming);
	}

	@Nullable
	@Override
	public DataType getType() {
		return DataType.STREAM;
	}
}
