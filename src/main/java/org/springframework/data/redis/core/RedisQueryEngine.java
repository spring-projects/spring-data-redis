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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.dao.DataAccessException;
import org.springframework.data.keyvalue.core.CriteriaAccessor;
import org.springframework.data.keyvalue.core.QueryEngine;
import org.springframework.data.keyvalue.core.SortAccessor;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.convert.RedisData;
import org.springframework.data.redis.repository.query.RedisOperationChain;

/**
 * Redis specific {@link QueryEngine} implementation.
 * 
 * @author Christoph Strobl
 */
public class RedisQueryEngine extends QueryEngine<RedisKeyValueAdapter, RedisOperationChain, Comparator<?>> {

	public RedisQueryEngine() {
		this(new RedisCriteriaAccessor(), null);
	}

	public RedisQueryEngine(CriteriaAccessor<RedisOperationChain> criteriaAccessor,
			SortAccessor<Comparator<?>> sortAccessor) {
		super(criteriaAccessor, sortAccessor);
	}

	@Override
	public Collection<?> execute(final RedisOperationChain criteria, Comparator<?> sort, int offset, int rows,
			final Serializable keyspace) {

		RedisCallback<List<Map<byte[], byte[]>>> callback = new RedisCallback<List<Map<byte[], byte[]>>>() {

			@Override
			public List<Map<byte[], byte[]>> doInRedis(RedisConnection connection) throws DataAccessException {

				String key = keyspace + ".";
				byte[][] keys = new byte[criteria.getSismember().size()][];
				int i = 0;
				for (Object o : criteria.getSismember()) {
					keys[i] = getAdapter().getConverter().getConversionService().convert(key + o, byte[].class);
					i++;
				}

				Set<byte[]> allKeys = connection.sInter(keys);

				byte[] keyspaceBin = getAdapter().getConverter().getConversionService().convert(keyspace + ":", byte[].class);

				final List<Map<byte[], byte[]>> rawData = new ArrayList<Map<byte[], byte[]>>();

				for (byte[] id : allKeys) {

					byte[] singleKey = Arrays.copyOf(keyspaceBin, id.length + keyspaceBin.length);
					System.arraycopy(id, 0, singleKey, keyspaceBin.length, id.length);

					rawData.add(connection.hGetAll(singleKey));
				}

				return rawData;

			}
		};

		List<Map<byte[], byte[]>> raw = this.getAdapter().execute(callback);

		List<Object> result = new ArrayList<Object>(raw.size());
		for (Map<byte[], byte[]> rawData : raw) {
			Object converted = this.getAdapter().getConverter().read(Object.class, new RedisData(rawData));
			if (converted != null) {
				result.add(converted);
			}
		}
		return result;
	}

	@Override
	public long count(final RedisOperationChain criteria, final Serializable keyspace) {

		return this.getAdapter().execute(new RedisCallback<Long>() {

			@Override
			public Long doInRedis(RedisConnection connection) throws DataAccessException {

				String key = keyspace + ".";
				byte[][] keys = new byte[criteria.getSismember().size()][];
				int i = 0;
				for (Object o : criteria.getSismember()) {
					keys[i] = getAdapter().getConverter().getConversionService().convert(key + o, byte[].class);
				}

				return (long) connection.sInter(keys).size();
			}
		});
	}

	static class RedisCriteriaAccessor implements CriteriaAccessor<RedisOperationChain> {

		@Override
		public RedisOperationChain resolve(KeyValueQuery<?> query) {
			return (RedisOperationChain) query.getCritieria();
		}
	}

}
