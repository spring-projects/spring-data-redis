/*
 * Copyright 2015-2020 the original author or authors.
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.keyvalue.core.CriteriaAccessor;
import org.springframework.data.keyvalue.core.QueryEngine;
import org.springframework.data.keyvalue.core.SortAccessor;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisZSetCommands.Range;
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
 * @author Yan Ma
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
				|| (CollectionUtils.isEmpty(criteria.getOrSismember())
						&& CollectionUtils.isEmpty(criteria.getSismember()))
						&& criteria.getNear() == null) {
			return (Collection<T>) getAdapter().getAllOf(keyspace, offset, rows);
		}

		RedisCallback<Map<byte[], Map<byte[], byte[]>>> callback = connection -> {

			List<byte[]> allKeys = new ArrayList<>();
			if (!criteria.getSismember().isEmpty()) {
				allKeys.addAll(getKeysFromIsMembers(connection, keyspace + ":", criteria.getSismember()));
			}

			if (!criteria.getOrSismember().isEmpty()) {
				allKeys.addAll(getKeysFromOrMembers(connection, keyspace + ":", criteria.getOrSismember()));
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


	private Collection<? extends byte[]> getKeysFromIsMembers(RedisConnection connection, String prefix,
			Set<PathAndValue> members) {
		
		// get simple types
		Set<PathAndValue> simpleQueries = new HashSet<PathAndValue>();
		// get range types
		Set<PathAndValue> rangeQueries = new HashSet<PathAndValue>();
		for (PathAndValue curr : members) {
			if (curr.getFirstValue() instanceof Range) {
				rangeQueries.add(curr);
			} else {
				simpleQueries.add(curr);
			}
		}
		
		Set<byte[]> sInter = new HashSet<byte[]>();
		// To limit the range query size should not exceeds 1.
		if(rangeQueries.size() > 1) {
			return sInter;
		}
		
		if (!simpleQueries.isEmpty()) {
			sInter = connection.sInter(keys(prefix, simpleQueries));
		}

		if (!simpleQueries.isEmpty() && sInter.isEmpty()) {
			// we do have something in simple queries but nothing found in the intersections.
			// no need of further checks. just return the empty set
			return sInter;
		}
		
		boolean rangeQueryOnly = simpleQueries.isEmpty();
		for (PathAndValue pathAndValue : rangeQueries) {// 0 or 1 loop only
			byte[] keyInByte = getAdapter().getConverter().getConversionService().convert(prefix + pathAndValue.getPath(),
					byte[].class);
			Set<byte[]> zRangeByScore = connection.zRangeByScore(keyInByte, (Range) pathAndValue.getFirstValue());
			if(rangeQueryOnly){
				// no simple query but range query only.
				sInter.addAll(zRangeByScore);
			} else {
				if (sInter.isEmpty()) {
					// in case we support multiple range queries later this could be a quick return
					// no more simple query overlapping with range query any more
					// return the empty set
					return sInter;
				} else {
					// remain intersections only
					Iterator<byte[]> itSimpleQuery = sInter.iterator();
					while (itSimpleQuery.hasNext()) {
						Iterator<byte[]> itTarget = zRangeByScore.iterator();
						byte[] source = itSimpleQuery.next();
						boolean isContained = false;
						while (itTarget.hasNext()) {
							byte[] target = itTarget.next();
							if (Arrays.equals(source, target)) {
								isContained = true;
								break;
							}
						}
						if (!isContained) {
							itSimpleQuery.remove();
						}
					}
				}
			}
		}

		return sInter;
	}

	private Collection<? extends byte[]> getKeysFromOrMembers(RedisConnection connection, String prefix,
			Set<PathAndValue> members) {

		// get simple types
		Set<PathAndValue> simpleQueries = new HashSet<PathAndValue>();
		// get range types
		Set<PathAndValue> rangeQueries = new HashSet<PathAndValue>();

		for (PathAndValue curr : members) {
			if (curr.getFirstValue() instanceof Range) {
				rangeQueries.add(curr);
			} else {
				simpleQueries.add(curr);
			}
		}

		Set<byte[]> sUnion = new HashSet<byte[]>();
		// To limit the range query size should not exceeds 1.
		if(rangeQueries.size() > 1) return sUnion;

		Set<byte[]> simpleUnion = connection.sUnion(keys(prefix, simpleQueries));
		Set<ByteBuffer> tmpSet = new HashSet<ByteBuffer>();
		for (PathAndValue pathAndValue : rangeQueries) {// 0 or 1 loop only
      byte[] keyInByte = getAdapter().getConverter().getConversionService()
          .convert(prefix + pathAndValue.getPath(), byte[].class);
      Set<byte[]> zRangeByScore = connection.zRangeByScore(keyInByte, (Range) pathAndValue.getFirstValue());
			// To merge range query with simple query union
      // O(n) + O(m), n = the number fo range query results, m = the number simple query union
      for(byte[] entry : zRangeByScore){
      	tmpSet.add(ByteBuffer.wrap(entry));
      }
		}
    for(byte[] entry: simpleUnion){
    	tmpSet.add(ByteBuffer.wrap(entry));
    }
    for(ByteBuffer bb : tmpSet){
    	sUnion.add(bb.array());
    }
      // this method time complexity is O(n x m)
//      Set<byte[]> extra = new HashSet<byte[]>();
//			Iterator<byte[]> itUnion = (Iterator<byte[]>) sUnion.iterator();
//			while (itUnion.hasNext()) {
//				Iterator<byte[]> itRange = (Iterator<byte[]>) zRangeByScore.iterator();
//				byte[] source = itUnion.next();
//				boolean isContained = false;
//				while (itRange.hasNext()) {
//					byte[] target = itRange.next();
//					if (Arrays.equals(source, target)) {
//						isContained = true;
//						break;
//					}
//				}
//				if (!isContained) {
//					extra.add(source);
//				}
//			}
		return sUnion;
	}
//	private Collection<? extends byte[]> getKeysFromOrMembers(RedisConnection connection, String prefix,
//			Set<Set<PathAndValue>> members) {
//		
//		Set<byte[]> sUnion = new HashSet<byte[]>();
//		for (Set<PathAndValue> isMemberSet : members) {
//			Collection<? extends byte[]> keysFromIsMembers = getKeysFromIsMembers(connection, prefix, isMemberSet);
//			if (sUnion.isEmpty()) {
//				sUnion.addAll(keysFromIsMembers);
//			} else {
//				// merge
//				Iterator<byte[]> it = (Iterator<byte[]>) keysFromIsMembers.iterator();
//				while (it.hasNext()) {
//					Iterator<byte[]> itTarget = (Iterator<byte[]>) sUnion.iterator();
//					byte[] source = it.next();
//					boolean isContained = false;
//					while (itTarget.hasNext()) {
//						byte[] target = itTarget.next();
//						if (Arrays.equals(source, target)) {
//							isContained = true;
//							break;
//						}
//					}
//					if (!isContained) {
//						sUnion.add(source);
//					}
//				}
//			}
//		}
//		return sUnion;
//	}

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
