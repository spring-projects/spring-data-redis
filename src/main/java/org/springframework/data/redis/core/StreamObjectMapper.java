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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.hash.ObjectHashMapper;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Utility to provide a {@link HashMapper} for Stream object conversion.
 * <p/>
 * This utility can use generic a {@link HashMapper} or adapt specifically to {@link ObjectHashMapper}'s requirement to
 * convert incoming data into byte arrays. This class can be subclassed to override template methods for specific object
 * mapping strategies.
 * 
 * @author Mark Paluch
 * @since 2.2
 * @see ObjectHashMapper
 * @see #doGetHashMapper(ConversionService, Class)
 */
class StreamObjectMapper {

	private final DefaultConversionService conversionService = new DefaultConversionService();
	private final RedisCustomConversions customConversions = new RedisCustomConversions();
	private final HashMapper<Object, Object, Object> mapper;
	private final @Nullable HashMapper<Object, Object, Object> objectHashMapper;

	/**
	 * Creates a new {@link StreamObjectMapper}.
	 * 
	 * @param mapper the configured {@link HashMapper}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	StreamObjectMapper(HashMapper<?, ?, ?> mapper) {

		Assert.notNull(mapper, "HashMapper must not be null");

		this.customConversions.registerConvertersIn(conversionService);
		this.mapper = (HashMapper) mapper;

		if (mapper instanceof ObjectHashMapper) {

			ObjectHashMapper ohm = (ObjectHashMapper) mapper;
			this.objectHashMapper = new HashMapper<Object, Object, Object>() {

				@Override
				public Map<Object, Object> toHash(Object object) {
					return (Map) ohm.toHash(object);
				}

				@Override
				public Object fromHash(Map<Object, Object> hash) {

					Map<byte[], byte[]> map = hash.entrySet().stream()
							.collect(Collectors.toMap(e -> conversionService.convert(e.getKey(), byte[].class),
									e -> conversionService.convert(e.getValue(), byte[].class)));

					return ohm.fromHash(map);
				}
			};
		} else {
			this.objectHashMapper = null;
		}
	}

	/**
	 * Convert the given {@link Record} into a {@link MapRecord}.
	 * 
	 * @param provider provider for {@link HashMapper} to apply mapping for {@link ObjectRecord}.
	 * @param source the source value.
	 * @return the converted {@link MapRecord}.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	static <K, V, HK, HV> MapRecord<K, HK, HV> toMapRecord(HashMapperProvider<HK, HV> provider, Record<K, V> source) {

		if (source instanceof ObjectRecord) {

			ObjectRecord entry = ((ObjectRecord) source);

			if (entry.getValue() instanceof Map) {
				return StreamRecords.newRecord().in(source.getStream()).withId(source.getId()).ofMap((Map) entry.getValue());
			}

			return entry.toMapRecord(provider.getHashMapper(entry.getValue().getClass()));
		}

		if (source instanceof MapRecord) {
			return (MapRecord<K, HK, HV>) source;
		}

		return Record.of(((HashMapper) provider.getHashMapper(source.getClass())).toHash(source))
				.withStreamKey(source.getStream());
	}

	/**
	 * Convert the given {@link Record} into an {@link ObjectRecord}.
	 * 
	 * @param provider provider for {@link HashMapper} to apply mapping for {@link ObjectRecord}.
	 * @param source the source value.
	 * @param targetType the desired target type.
	 * @return the converted {@link ObjectRecord}.
	 */
	static <K, V, HK, HV> ObjectRecord<K, V> toObjectRecord(HashMapperProvider<HK, HV> provider,
			MapRecord<K, HK, HV> source, Class<V> targetType) {
		return source.toObjectRecord(provider.getHashMapper(targetType));
	}

	/**
	 * Map a {@link List} of {@link MapRecord}s to a {@link List} of {@link ObjectRecord}. Optimizes for empty,
	 * single-element and multi-element list transformation.l
	 * 
	 * @param records the {@link MapRecord} that should be mapped.
	 * @param hashMapperProvider the provider to obtain the actual {@link HashMapper} from. Must not be {@literal null}.
	 * @param targetType the requested {@link Class target type}.
	 * @return the resulting {@link List} of {@link ObjectRecord} or {@literal null} if {@code records} was
	 *         {@literal null}.
	 */
	@Nullable
	static <K, V, HK, HV> List<ObjectRecord<K, V>> map(@Nullable List<MapRecord<K, HK, HV>> records,
			HashMapperProvider<HK, HV> hashMapperProvider, Class<V> targetType) {

		if (records == null) {
			return null;
		}

		if (records.isEmpty()) {
			return Collections.emptyList();
		}

		if (records.size() == 1) {
			return Collections.singletonList(toObjectRecord(hashMapperProvider, records.get(0), targetType));
		}

		List<ObjectRecord<K, V>> transformed = new ArrayList<>(records.size());
		HashMapper<V, HK, HV> hashMapper = hashMapperProvider.getHashMapper(targetType);

		for (MapRecord<K, HK, HV> record : records) {
			transformed.add(record.toObjectRecord(hashMapper));
		}

		return transformed;
	}

	@SuppressWarnings("unchecked")
	final <V, HK, HV> HashMapper<V, HK, HV> getHashMapper(Class<V> targetType) {
		return (HashMapper) doGetHashMapper(conversionService, targetType);
	}

	/**
	 * Returns the actual {@link HashMapper}. Can be overridden by subclasses.
	 * 
	 * @param conversionService the used {@link ConversionService}.
	 * @param targetType the target type.
	 * @return obtain the {@link HashMapper} for a certain type.
	 */
	protected HashMapper<?, ?, ?> doGetHashMapper(ConversionService conversionService, Class<?> targetType) {
		return this.objectHashMapper != null ? objectHashMapper : this.mapper;
	}

	/**
	 * Check if the given type is a simple type as in
	 * {@link org.springframework.data.convert.CustomConversions#isSimpleType(Class)}.
	 *
	 * @param targetType the type to inspect. Must not be {@literal null}.
	 * @return {@literal true} if {@link Class targetType} is a simple type.
	 * @see org.springframework.data.convert.CustomConversions#isSimpleType(Class)
	 */
	boolean isSimpleType(Class<?> targetType) {
		return customConversions.isSimpleType(targetType);
	}

	/**
	 * @return used {@link ConversionService}.
	 */
	ConversionService getConversionService() {
		return this.conversionService;
	}
}
