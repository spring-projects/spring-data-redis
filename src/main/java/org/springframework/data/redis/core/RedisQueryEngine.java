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
package org.springframework.data.redis.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.core.CriteriaAccessor;
import org.springframework.data.keyvalue.core.QueryEngine;
import org.springframework.data.keyvalue.core.SortAccessor;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.convert.RedisData;
import org.springframework.data.redis.repository.query.RedisOperationChain;
import org.springframework.data.redis.repository.query.RedisOperationChain.PathAndValue;
import org.springframework.data.redis.util.ByteUtils;

/**
 * Redis specific {@link QueryEngine} implementation.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
class RedisQueryEngine extends QueryEngine<RedisKeyValueAdapter, RedisOperationChain, Comparator<?>> {

	/**
	 * Creates new {@link RedisQueryEngine} with defaults.
	 */
	public RedisQueryEngine() {
		this(new RedisCriteriaAccessor(), null);
	}

	/**
	 * Creates new {@link RedisQueryEngine}.
	 * 
	 * @param criteriaAccessor
	 * @param sortAccessor
	 * @see QueryEngine#QueryEngine(CriteriaAccessor, SortAccessor)
	 */
	public RedisQueryEngine(CriteriaAccessor<RedisOperationChain> criteriaAccessor,
			SortAccessor<Comparator<?>> sortAccessor) {
		super(criteriaAccessor, sortAccessor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.QueryEngine#execute(java.lang.Object, java.lang.Object, int, int, java.io.Serializable, java.lang.Class)
	 */
	public <T> Collection<T> execute(final RedisOperationChain criteria, final Comparator<?> sort, final int offset,
			final int rows, final Serializable keyspace, Class<T> type) {

		RedisCallback<Map<byte[], Map<byte[], byte[]>>> callback = new RedisCallback<Map<byte[], Map<byte[], byte[]>>>() {

			@Override
			public Map<byte[], Map<byte[], byte[]>> doInRedis(RedisConnection connection) throws DataAccessException {

				String key = keyspace + ":";
				byte[][] keys = new byte[criteria.getSismember().size()][];
				int i = 0;
				for (Object o : criteria.getSismember()) {
					keys[i] = getAdapter().getConverter().getConversionService().convert(key + o, byte[].class);
					i++;
				}

				List<byte[]> allKeys = new ArrayList<byte[]>();
				if (!criteria.getSismember().isEmpty()) {
					allKeys.addAll(connection.sInter(keys(keyspace + ":", criteria.getSismember())));
				}
				if (!criteria.getOrSismember().isEmpty()) {
					allKeys.addAll(connection.sUnion(keys(keyspace + ":", criteria.getOrSismember())));
				}

				byte[] keyspaceBin = getAdapter().getConverter().getConversionService().convert(keyspace + ":", byte[].class);

				final Map<byte[], Map<byte[], byte[]>> rawData = new LinkedHashMap<byte[], Map<byte[], byte[]>>();

				if (allKeys.size() == 0 || allKeys.size() < offset) {
					return Collections.emptyMap();
				}

				if (offset >= 0 && rows > 0) {
					allKeys = allKeys.subList(Math.max(0, offset), Math.min(offset + rows, allKeys.size()));
				}
				for (byte[] id : allKeys) {

					byte[] singleKey = ByteUtils.concat(keyspaceBin, id);
					rawData.put(id, connection.hGetAll(singleKey));
				}

				return rawData;

			}
		};

		Map<byte[], Map<byte[], byte[]>> raw = this.getAdapter().execute(callback);

		List<T> result = new ArrayList<T>(raw.size());
		for (Map.Entry<byte[], Map<byte[], byte[]>> entry : raw.entrySet()) {

			RedisData data = new RedisData(entry.getValue());
			data.setId(getAdapter().getConverter().getConversionService().convert(entry.getKey(), String.class));
			data.setKeyspace(keyspace.toString());

			T converted = this.getAdapter().getConverter().read(type, data);

			if (converted != null) {
				result.add(converted);
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.QueryEngine#execute(java.lang.Object, java.lang.Object, int, int, java.io.Serializable)
	 */
	@Override
	public Collection<?> execute(final RedisOperationChain criteria, Comparator<?> sort, int offset, int rows,
			final Serializable keyspace) {
		return execute(criteria, sort, offset, rows, keyspace, Object.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.QueryEngine#count(java.lang.Object, java.io.Serializable)
	 */
	@Override
	public long count(final RedisOperationChain criteria, final Serializable keyspace) {

		return this.getAdapter().execute(new RedisCallback<Long>() {

			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {

				String key = keyspace + ":";
				byte[][] keys = new byte[criteria.getSismember().size()][];
				int i = 0;
				for (Object o : criteria.getSismember()) {
					keys[i] = getAdapter().getConverter().getConversionService().convert(key + o, byte[].class);
				}

				return (long) connection.sInter(keys).size();
			}
		});
	}

	private byte[][] keys(String prefix, Collection<PathAndValue> source) {

		byte[][] keys = new byte[source.size()][];
		int i = 0;
		for (PathAndValue pathAndValue : source) {

			byte[] convertedValue = getAdapter().getConverter().getConversionService()
					.convert(pathAndValue.getFirstValue(), byte[].class);
			byte[] fullPath = getAdapter().getConverter().getConversionService()
					.convert(prefix + pathAndValue.getPath() + ":", byte[].class);

			keys[i] = ByteUtils.concat(fullPath, convertedValue);
			i++;
		}
		return keys;
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class RedisCriteriaAccessor implements CriteriaAccessor<RedisOperationChain> {

		@Override
		public RedisOperationChain resolve(KeyValueQuery<?> query) {
			return (RedisOperationChain) query.getCritieria();
		}
	}

}
