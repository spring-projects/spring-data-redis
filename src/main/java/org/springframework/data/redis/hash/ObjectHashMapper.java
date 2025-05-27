/*
 * Copyright 2016-2025 the original author or authors.
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
package org.springframework.data.redis.hash;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.redis.core.convert.IndexResolver;
import org.springframework.data.redis.core.convert.IndexedData;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.RedisConverter;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.core.convert.RedisData;
import org.springframework.data.redis.core.convert.ReferenceResolver;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;

/**
 * {@link HashMapper} based on {@link MappingRedisConverter}. Supports nested properties and simple types like
 * {@link String}.
 *
 * <pre>
 * <code>
 * class Person {
 *
 *   String firstname;
 *   String lastname;
 *
 *   List&lt;String&gt; nicknames;
 *   List&lt;Person&gt; coworkers;
 *
 *   Address address;
 * }
 * </code>
 * </pre>
 *
 * The above is represented as:
 *
 * <pre>
 * <code>
 * _class=org.example.Person
 * firstname=rand
 * lastname=al'thor
 * coworkers.[0].firstname=mat
 * coworkers.[0].nicknames.[0]=prince of the ravens
 * coworkers.[1].firstname=perrin
 * coworkers.[1].address.city=two rivers
 * </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.8
 */
public class ObjectHashMapper implements HashMapper<Object, byte[], byte[]> {

	@Nullable private volatile static ObjectHashMapper sharedInstance;

	private final RedisConverter converter;

	/**
	 * Creates new {@link ObjectHashMapper}.
	 */
	public ObjectHashMapper() {
		this(new RedisCustomConversions());
	}

	/**
	 * Creates a new {@link ObjectHashMapper} using the given {@link RedisConverter} for conversion.
	 *
	 * @param converter must not be {@literal null}.
	 * @throws IllegalArgumentException if the given {@literal converter} is {@literal null}.
	 * @since 2.4
	 */
	public ObjectHashMapper(RedisConverter converter) {

		Assert.notNull(converter, "Converter must not be null");
		this.converter = converter;
	}

	/**
	 * Creates new {@link ObjectHashMapper}.
	 *
	 * @param customConversions can be {@literal null}.
	 * @since 2.0
	 */
	public ObjectHashMapper(@Nullable CustomConversions customConversions) {

		MappingRedisConverter mappingConverter = new MappingRedisConverter(new RedisMappingContext(),
				new NoOpIndexResolver(), new NoOpReferenceResolver());
		mappingConverter.setCustomConversions(customConversions == null ? new RedisCustomConversions() : customConversions);
		mappingConverter.afterPropertiesSet();

		converter = mappingConverter;
	}

	/**
	 * Return a shared default {@link ObjectHashMapper} instance, lazily building it once needed.
	 * <p>
	 * <b>NOTE:</b> We highly recommend constructing individual {@link ObjectHashMapper} instances for customization
	 * purposes. This accessor is only meant as a fallback for code paths which need simple type coercion but cannot
	 * access a longer-lived {@link ObjectHashMapper} instance any other way.
	 *
	 * @return the shared {@link ObjectHashMapper} instance (never {@literal null}).
	 * @since 2.4
	 */
	public static ObjectHashMapper getSharedInstance() {

		ObjectHashMapper cs = sharedInstance;
		if (cs == null) {
			synchronized (ObjectHashMapper.class) {
				cs = sharedInstance;
				if (cs == null) {
					cs = new ObjectHashMapper();
					sharedInstance = cs;
				}
			}
		}
		return cs;
	}

	@Override
	@Contract("null -> !null")
	public Map<byte[], byte[]> toHash(@Nullable Object source) {

		if (source == null) {
			return Collections.emptyMap();
		}

		RedisData sink = new RedisData();
		converter.write(source, sink);
		return sink.getBucket().rawMap();
	}

	@Override
	@Contract("null -> null")
	public @Nullable Object fromHash(@Nullable Map<byte[], byte[]> hash) {

		if (hash == null || hash.isEmpty()) {
			return null;
		}

		return converter.read(Object.class, new RedisData(hash));
	}

	/**
	 * Convert a {@code hash} (map) to an object and return the casted result.
	 *
	 * @param hash
	 * @param type
	 * @param <T>
	 * @return
	 */
	public <T> @Nullable T fromHash(Map<byte[], byte[]> hash, Class<T> type) {
		return type.cast(fromHash(hash));
	}

	/**
	 * {@link ReferenceResolver} implementation always returning an empty {@link Map}.
	 *
	 * @author Christoph Strobl
	 */
	private static class NoOpReferenceResolver implements ReferenceResolver {

		private static final Map<byte[], byte[]> NO_REFERENCE = Collections.emptyMap();

		@Override
		public Map<byte[], byte[]> resolveReference(Object id, String keyspace) {
			return NO_REFERENCE;
		}
	}

	/**
	 * {@link IndexResolver} always returning an empty {@link Set}.
	 *
	 * @author Christoph Strobl
	 */
	private static class NoOpIndexResolver implements IndexResolver {

		private static final Set<IndexedData> NO_INDEXES = Collections.emptySet();

		@Override
		public Set<IndexedData> resolveIndexesFor(TypeInformation<?> typeInformation, @Nullable Object value) {
			return NO_INDEXES;
		}

		@Override
		public Set<IndexedData> resolveIndexesFor(String keyspace, String path, TypeInformation<?> typeInformation,
				@Nullable Object value) {
			return NO_INDEXES;
		}
	}
}
