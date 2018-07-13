/*
 * Copyright 2017 the original author or authors.
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

import java.util.Collections;
import java.util.List;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.convert.TypeInformationMapper;
import org.springframework.data.mapping.Alias;
import org.springframework.data.mapping.PersistentEntity;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.redis.core.convert.Bucket.BucketPropertyPath;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link RedisTypeMapper} allowing configuration of the key to lookup and store type
 * information via {@link BucketPropertyPath} in buckets. The key defaults to {@link #DEFAULT_TYPE_KEY}. Actual
 * type-to-{@code byte[]} conversion and back is done in {@link BucketTypeAliasAccessor}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
public class DefaultRedisTypeMapper extends DefaultTypeMapper<BucketPropertyPath> implements RedisTypeMapper {

	public static final String DEFAULT_TYPE_KEY = "_class";

	private final @Nullable String typeKey;

	/**
	 * Create a new {@link DefaultRedisTypeMapper} using {@link #DEFAULT_TYPE_KEY} to exchange type hints.
	 */
	public DefaultRedisTypeMapper() {
		this(DEFAULT_TYPE_KEY);
	}

	/**
	 * Create a new {@link DefaultRedisTypeMapper} given {@code typeKey} to exchange type hints. Does not consider type
	 * hints if {@code typeKey} is {@literal null}.
	 *
	 * @param typeKey the type key can be {@literal null} to skip type hinting.
	 */
	public DefaultRedisTypeMapper(@Nullable String typeKey) {
		this(typeKey, Collections.singletonList(new SimpleTypeInformationMapper()));
	}

	/**
	 * Create a new {@link DefaultRedisTypeMapper} given {@code typeKey} to exchange type hints and
	 * {@link MappingContext}. Does not consider type hints if {@code typeKey} is {@literal null}. {@link MappingContext}
	 * is used to obtain entity-based aliases
	 *
	 * @param typeKey the type key can be {@literal null} to skip type hinting.
	 * @param mappingContext must not be {@literal null}.
	 * @see org.springframework.data.annotation.TypeAlias
	 */
	public DefaultRedisTypeMapper(@Nullable String typeKey,
			MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext) {
		this(typeKey, new BucketTypeAliasAccessor(typeKey, getConversionService()), mappingContext,
				Collections.singletonList(new SimpleTypeInformationMapper()));
	}

	/**
	 * Create a new {@link DefaultRedisTypeMapper} given {@code typeKey} to exchange type hints and {@link List} of
	 * {@link TypeInformationMapper}. Does not consider type hints if {@code typeKey} is {@literal null}.
	 * {@link MappingContext} is used to obtain entity-based aliases
	 *
	 * @param typeKey the type key can be {@literal null} to skip type hinting.
	 * @param mappers must not be {@literal null}.
	 */
	public DefaultRedisTypeMapper(@Nullable String typeKey, List<? extends TypeInformationMapper> mappers) {
		this(typeKey, new BucketTypeAliasAccessor(typeKey, getConversionService()), null, mappers);
	}

	private DefaultRedisTypeMapper(@Nullable String typeKey, TypeAliasAccessor<BucketPropertyPath> accessor,
			@Nullable MappingContext<? extends PersistentEntity<?, ?>, ?> mappingContext,
			List<? extends TypeInformationMapper> mappers) {

		super(accessor, mappingContext, mappers);

		this.typeKey = typeKey;
	}

	private static GenericConversionService getConversionService() {

		GenericConversionService conversionService = new GenericConversionService();
		new RedisCustomConversions().registerConvertersIn(conversionService);

		return conversionService;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.RedisTypeMapper#isTypeKey(java.lang.String)
	 */
	public boolean isTypeKey(@Nullable String key) {
		return key != null && typeKey != null && key.endsWith(typeKey);
	}

	/**
	 * {@link TypeAliasAccessor} to store aliases in a {@link Bucket}.
	 *
	 * @author Mark Paluch
	 */
	static final class BucketTypeAliasAccessor implements TypeAliasAccessor<BucketPropertyPath> {

		private final @Nullable String typeKey;

		private final ConversionService conversionService;

		BucketTypeAliasAccessor(@Nullable String typeKey, ConversionService conversionService) {

			Assert.notNull(conversionService, "ConversionService must not be null!");

			this.typeKey = typeKey;
			this.conversionService = conversionService;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#readAliasFrom(java.lang.Object)
		 */
		public Alias readAliasFrom(BucketPropertyPath source) {

			if (typeKey == null || source instanceof List) {
				return Alias.NONE;
			}

			byte[] bytes = source.get(typeKey);

			if (bytes != null) {
				return Alias.ofNullable(conversionService.convert(bytes, String.class));
			}

			return Alias.NONE;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.convert.TypeAliasAccessor#writeTypeTo(java.lang.Object, java.lang.Object)
		 */
		public void writeTypeTo(BucketPropertyPath sink, Object alias) {

			if (typeKey != null) {

				if (alias instanceof byte[]) {
					sink.put(typeKey, (byte[]) alias);
				} else {
					sink.put(typeKey, conversionService.convert(alias, byte[].class));
				}
			}
		}
	}
}
