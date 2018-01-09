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
package org.springframework.data.redis.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.keyvalue.core.CriteriaAccessor;
import org.springframework.data.keyvalue.core.QueryEngine;
import org.springframework.data.keyvalue.core.SortAccessor;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.core.convert.GeoIndexedPropertyValue;
import org.springframework.data.redis.core.convert.RedisData;
import org.springframework.data.redis.repository.query.RedisOperationChain;
import org.springframework.data.redis.repository.query.RedisOperationChain.NearPath;
import org.springframework.data.redis.repository.query.RedisOperationChain.PathAndValue;
import org.springframework.data.redis.util.ByteUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

/**
 * Redis specific {@link QueryEngine} implementation.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
class RedisQueryEngine extends QueryEngine<RedisKeyValueAdapter, RedisOperationChain, Comparator<?>> {

	/**
	 * Creates new {@link RedisQueryEngine} with defaults.
	 */
	RedisQueryEngine() {
		this(new RedisCriteriaAccessor(), null);
	}

	/**
	 * Creates new {@link RedisQueryEngine}.
	 *
	 * @param criteriaAccessor
	 * @param sortAccessor
	 * @see QueryEngine#QueryEngine(CriteriaAccessor, SortAccessor)
	 */
	private RedisQueryEngine(CriteriaAccessor<RedisOperationChain> criteriaAccessor,
			@Nullable SortAccessor<Comparator<?>> sortAccessor) {
		super(criteriaAccessor, sortAccessor);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.QueryEngine#execute(java.lang.Object, java.lang.Object, int, int, java.lang.String, java.lang.Class)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> Collection<T> execute(RedisOperationChain criteria, Comparator<?> sort, long offset, int rows,
			String keyspace, Class<T> type) {

		if (criteria == null
				|| (CollectionUtils.isEmpty(criteria.getOrSismember()) && CollectionUtils.isEmpty(criteria.getSismember()))
						&& criteria.getNear() == null) {
			return (Collection<T>) getAdapter().getAllOf(keyspace, offset, rows);
		}

		RedisCallback<Map<byte[], Map<byte[], byte[]>>> callback = connection -> {

			List<byte[]> allKeys = new ArrayList<>();
			if (!criteria.getSismember().isEmpty()) {
				allKeys.addAll(connection.sInter(keys(keyspace + ":", criteria.getSismember())));
			}

			if (!criteria.getOrSismember().isEmpty()) {
				allKeys.addAll(connection.sUnion(keys(keyspace + ":", criteria.getOrSismember())));
			}

			if (criteria.getNear() != null) {

				GeoResults<GeoLocation<byte[]>> x = connection.geoRadius(geoKey(keyspace + ":", criteria.getNear()),
						new Circle(criteria.getNear().getPoint(), criteria.getNear().getDistance()));
				for (GeoResult<GeoLocation<byte[]>> y : x) {
					allKeys.add(y.getContent().getName());
				}
			}

			byte[] keyspaceBin = getAdapter().getConverter().getConversionService().convert(keyspace + ":", byte[].class);

			Map<byte[], Map<byte[], byte[]>> rawData = new LinkedHashMap<>();

			if (allKeys.isEmpty() || allKeys.size() < offset) {
				return Collections.emptyMap();
			}

			int offsetToUse = Math.max(0, (int) offset);
			if (rows > 0) {
				allKeys = allKeys.subList(Math.max(0, offsetToUse), Math.min(offsetToUse + rows, allKeys.size()));
			}
			for (byte[] id : allKeys) {

				byte[] singleKey = ByteUtils.concat(keyspaceBin, id);
				rawData.put(id, connection.hGetAll(singleKey));
			}

			return rawData;
		};

		Map<byte[], Map<byte[], byte[]>> raw = this.getAdapter().execute(callback);

		List<T> result = new ArrayList<>(raw.size());
		for (Map.Entry<byte[], Map<byte[], byte[]>> entry : raw.entrySet()) {

			RedisData data = new RedisData(entry.getValue());
			data.setId(getAdapter().getConverter().getConversionService().convert(entry.getKey(), String.class));
			data.setKeyspace(keyspace);

			T converted = this.getAdapter().getConverter().read(type, data);

			if (converted != null) {
				result.add(converted);
			}
		}
		return result;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.QueryEngine#execute(java.lang.Object, java.lang.Object, int, int, java.lang.String)
	 */
	@Override
	public Collection<?> execute(RedisOperationChain criteria, Comparator<?> sort, long offset, int rows,
			String keyspace) {
		return execute(criteria, sort, offset, rows, keyspace, Object.class);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.QueryEngine#count(java.lang.Object, java.lang.String)
	 */
	@Override
	public long count(RedisOperationChain criteria, String keyspace) {

		if (criteria == null || criteria.isEmpty()) {
			return this.getAdapter().count(keyspace);
		}

		return this.getAdapter().execute(connection -> {

			long result = 0;

			if (!criteria.getOrSismember().isEmpty()) {
				result += connection.sUnion(keys(keyspace + ":", criteria.getOrSismember())).size();
			}

			if (!criteria.getSismember().isEmpty()) {
				result += connection.sInter(keys(keyspace + ":", criteria.getSismember())).size();
			}

			return result;
		});
	}

	private byte[][] keys(String prefix, Collection<PathAndValue> source) {

		byte[][] keys = new byte[source.size()][];
		int i = 0;
		for (PathAndValue pathAndValue : source) {

			byte[] convertedValue = getAdapter().getConverter().getConversionService().convert(pathAndValue.getFirstValue(),
					byte[].class);
			byte[] fullPath = getAdapter().getConverter().getConversionService()
					.convert(prefix + pathAndValue.getPath() + ":", byte[].class);

			keys[i] = ByteUtils.concat(fullPath, convertedValue);
			i++;
		}
		return keys;
	}

	private byte[] geoKey(String prefix, NearPath source) {

		String path = GeoIndexedPropertyValue.geoIndexName(source.getPath());
		return getAdapter().getConverter().getConversionService().convert(prefix + path, byte[].class);

	}

	/**
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class RedisCriteriaAccessor implements CriteriaAccessor<RedisOperationChain> {

		@Override
		public RedisOperationChain resolve(KeyValueQuery<?> query) {
			return (RedisOperationChain) query.getCriteria();
		}
	}
}
