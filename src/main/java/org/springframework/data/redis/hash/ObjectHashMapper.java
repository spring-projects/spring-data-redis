/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.hash;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.convert.CustomConversions;
import org.springframework.data.redis.core.convert.IndexResolver;
import org.springframework.data.redis.core.convert.IndexedData;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.core.convert.RedisData;
import org.springframework.data.redis.core.convert.ReferenceResolver;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

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

	private final MappingRedisConverter converter;

	/**
	 * Creates new {@link ObjectHashMapper}.
	 */
	public ObjectHashMapper() {
		this(new RedisCustomConversions());
	}

	/**
	 * Creates new {@link ObjectHashMapper}.
	 *
	 * @param customConversions can be {@literal null}.
	 * @deprecated since 2.0, use {@link #ObjectHashMapper(org.springframework.data.convert.CustomConversions)}.
	 */
	@Deprecated
	public ObjectHashMapper(CustomConversions customConversions) {
		this((org.springframework.data.convert.CustomConversions) customConversions);
	}

	/**
	 * Creates new {@link ObjectHashMapper}.
	 *
	 * @param customConversions can be {@literal null}.
	 * @since 2.0
	 */
	public ObjectHashMapper(@Nullable org.springframework.data.convert.CustomConversions customConversions) {

		MappingRedisConverter mappingConverter = new MappingRedisConverter(new RedisMappingContext(),
				new NoOpIndexResolver(), new NoOpReferenceResolver());
		mappingConverter.setCustomConversions(customConversions == null ? new RedisCustomConversions() : customConversions);
		mappingConverter.afterPropertiesSet();

		converter = mappingConverter;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.hash.HashMapper#toHash(java.lang.Object)
	 */
	@Override
	public Map<byte[], byte[]> toHash(Object source) {

		if (source == null) {
			return Collections.emptyMap();
		}

		RedisData sink = new RedisData();
		converter.write(source, sink);
		return sink.getBucket().rawMap();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.hash.HashMapper#fromHash(java.util.Map)
	 */
	@Override
	public Object fromHash(Map<byte[], byte[]> hash) {

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
	public <T> T fromHash(Map<byte[], byte[]> hash, Class<T> type) {
		return type.cast(fromHash(hash));
	}

	/**
	 * {@link ReferenceResolver} implementation always returning an empty {@link Map}.
	 *
	 * @author Christoph Strobl
	 */
	private static class NoOpReferenceResolver implements ReferenceResolver {

		private static final Map<byte[], byte[]> NO_REFERENCE = Collections.emptyMap();

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.core.convert.ReferenceResolver#resolveReference(java.lang.Object, java.lang.String)
		 */
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

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.core.convert.IndexResolver#resolveIndexesFor(org.springframework.data.util.TypeInformation, java.lang.Object)
		 */
		@Override
		public Set<IndexedData> resolveIndexesFor(TypeInformation<?> typeInformation, Object value) {
			return NO_INDEXES;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.core.convert.IndexResolver#resolveIndexesFor(java.lang.String, java.lang.String, org.springframework.data.util.TypeInformation, java.lang.Object)
		 */
		@Override
		public Set<IndexedData> resolveIndexesFor(String keyspace, String path, TypeInformation<?> typeInformation,
				Object value) {
			return NO_INDEXES;
		}
	}
}
