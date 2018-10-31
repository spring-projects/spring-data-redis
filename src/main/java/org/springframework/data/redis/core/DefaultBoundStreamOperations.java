/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.redis.core;

import java.util.List;
import java.util.Map;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisStreamCommands.Consumer;
import org.springframework.data.redis.connection.RedisStreamCommands.MapRecord;
import org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.RecordId;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamOffset;
import org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions;
import org.springframework.data.redis.connection.RedisZSetCommands.Limit;
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

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#acknowledge(java.lang.String, java.lang.String[])
	 */
	@Nullable
	@Override
	public Long acknowledge(String group, String... recordIds) {
		return ops.acknowledge(getKey(), group, recordIds);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#add(java.util.Map)
	 */
	@Nullable
	@Override
	public RecordId add(Map<HK, HV> body) {
		return ops.add(getKey(), body);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#delete(java.lang.String[])
	 */
	@Nullable
	@Override
	public Long delete(String... recordIds) {
		return ops.delete(getKey(), recordIds);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#createGroup(org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset, java.lang.String)
	 */
	@Nullable
	@Override
	public String createGroup(ReadOffset readOffset, String group) {
		return ops.createGroup(getKey(), readOffset, group);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#deleteConsumer(org.springframework.data.redis.connection.RedisStreamCommands.Consumer)
	 */
	@Nullable
	@Override
	public Boolean deleteConsumer(Consumer consumer) {
		return ops.deleteConsumer(getKey(), consumer);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#destroyGroup(java.lang.String)
	 */
	@Nullable
	@Override
	public Boolean destroyGroup(String group) {
		return ops.destroyGroup(getKey(), group);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#size()
	 */
	@Nullable
	@Override
	public Long size() {
		return ops.size(getKey());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#range(org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Nullable
	@Override
	public List<MapRecord<K, HK, HV>> range(Range<String> range, Limit limit) {
		return ops.range(getKey(), range, limit);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset)
	 */
	@Nullable
	@Override
	public List<MapRecord<K, HK, HV>> read(StreamReadOptions readOptions, ReadOffset readOffset) {
		return ops.read(readOptions, StreamOffset.create(getKey(), readOffset));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#read(org.springframework.data.redis.connection.RedisStreamCommands.Consumer, org.springframework.data.redis.connection.RedisStreamCommands.StreamReadOptions, org.springframework.data.redis.connection.RedisStreamCommands.ReadOffset)
	 */
	@Nullable
	@Override
	public List<MapRecord<K, HK, HV>> read(Consumer consumer, StreamReadOptions readOptions, ReadOffset readOffset) {
		return ops.read(consumer, readOptions, StreamOffset.create(getKey(), readOffset));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#reverseRange(org.springframework.data.domain.Range, org.springframework.data.redis.connection.RedisZSetCommands.Limit)
	 */
	@Nullable
	@Override
	public List<MapRecord<K, HK, HV>> reverseRange(Range<String> range, Limit limit) {
		return ops.reverseRange(getKey(), range, limit);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundStreamOperations#trim(long)
	 */
	@Nullable
	@Override
	public Long trim(long count) {
		return ops.trim(getKey(), count);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.BoundKeyOperations#getType()
	 */
	@Nullable
	@Override
	public DataType getType() {
		return DataType.STREAM;
	}
}
