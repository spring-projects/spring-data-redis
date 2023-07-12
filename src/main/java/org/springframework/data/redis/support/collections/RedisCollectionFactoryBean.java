/*
 * Copyright 2011-2023 the original author or authors.
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
package org.springframework.data.redis.support.collections;

import java.util.function.Supplier;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.SmartFactoryBean;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.util.Lazy;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Factory bean that facilitates creation of Redis-based collections. Supports list, set, zset (or sortedSet), map (or
 * hash) and properties. Uses the key and {@link CollectionType} to determine what collection type to use. The factory
 * verifies the key type if a {@link CollectionType} is specified. Defaults to {@link CollectionType#LIST}.
 *
 * @author Costin Leau
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see RedisStore
 */
public class RedisCollectionFactoryBean implements SmartFactoryBean<RedisStore>, BeanNameAware, InitializingBean {

	/**
	 * Collection types supported by this factory.
	 *
	 * @author Costin Leau
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	public enum CollectionType {
		LIST {

			@Override
			public DataType dataType() {
				return DataType.LIST;
			}
		},
		SET {

			@Override
			public DataType dataType() {
				return DataType.SET;
			}
		},
		ZSET {

			@Override
			public DataType dataType() {
				return DataType.ZSET;
			}
		},
		MAP {

			@Override
			public DataType dataType() {
				return DataType.HASH;
			}
		},
		PROPERTIES {

			@Override
			public DataType dataType() {
				return DataType.HASH;
			}
		};

		abstract DataType dataType();

		/**
		 * Attempt to find a {@link CollectionType} by {@link DataType}. Defaults to {@link Supplier ifNotFound} when
		 * {@code dataType} is {@literal null} or the collection type cannot be determined.
		 *
		 * @param dataType the {@link DataType} to look up.
		 * @param ifNotFound supplier for a default value.
		 * @since 3.2
		 */
		static CollectionType findCollectionType(@Nullable DataType dataType, Supplier<CollectionType> ifNotFound) {

			if (dataType == null) {
				return ifNotFound.get();
			}

			for (CollectionType collectionType : values()) {
				if (collectionType.dataType() == dataType) {
					return collectionType;
				}
			}

			return ifNotFound.get();
		}
	}

	private @Nullable CollectionType type;
	private @Nullable RedisTemplate<String, ?> template;
	private @Nullable String key;
	private @Nullable String beanName;

	private @Nullable Lazy<RedisStore> store;

	@Override
	public void afterPropertiesSet() {

		if (!StringUtils.hasText(key)) {
			key = beanName;
		}

		Assert.hasText(key, "Collection key is required - no key or bean name specified");
		Assert.notNull(template, "Redis template is required");

		store = Lazy.of(() -> {

			DataType keyType = template.type(key);

			// can't create store
			Assert.isTrue(!DataType.STREAM.equals(keyType), "Cannot create store on keys of type 'STREAM'");

			if (this.type == null) {
				this.type = CollectionType.findCollectionType(keyType, () -> CollectionType.LIST);
			}

			if (keyType != null && DataType.NONE != keyType && this.type.dataType() != keyType) {
				throw new IllegalArgumentException(
						"Cannot create collection type '%s' for a key containing '%s'".formatted(this.type, keyType));
			}

			return createStore(this.type, key, template);
		});
	}

	private RedisStore createStore(CollectionType collectionType, String key, RedisOperations<String, ?> operations) {

		return switch (collectionType) {
			case LIST -> RedisList.create(key, operations);
			case SET -> new DefaultRedisSet<>(key, operations);
			case ZSET -> RedisZSet.create(key, operations);
			case PROPERTIES -> new RedisProperties(key, operations);
			case MAP -> new DefaultRedisMap<>(key, operations);
		};
	}

	@Override
	public RedisStore getObject() {

		Assert.state(store != null,
				"RedisCollectionFactoryBean is not initialized. Ensure to initialize this factory by calling afterPropertiesSet() before obtaining the factory object.");
		return store.get();
	}

	@Override
	public Class<?> getObjectType() {
		return (store != null ? store.get().getClass() : RedisStore.class);
	}

	@Override
	public void setBeanName(String name) {
		this.beanName = name;
	}

	/**
	 * Sets the store type. Used if the key does not exist.
	 *
	 * @param type The type to set.
	 */
	public void setType(CollectionType type) {
		this.type = type;
	}

	/**
	 * Sets the template used by the resulting store.
	 *
	 * @param template The template to set.
	 */
	public void setTemplate(RedisTemplate<String, ?> template) {
		this.template = template;
	}

	/**
	 * Sets the key of the store.
	 *
	 * @param key The key to set.
	 */
	public void setKey(String key) {
		this.key = key;
	}
}
