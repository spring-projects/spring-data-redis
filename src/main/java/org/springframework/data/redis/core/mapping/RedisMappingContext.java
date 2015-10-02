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
package org.springframework.data.redis.core.mapping;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;

import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.core.mapping.KeySpaceResolver;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.redis.core.TimeToLiveResolver;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 */
public class RedisMappingContext extends KeyValueMappingContext {

	private MappingConfiguration mappingConfiguration;
	private KeySpaceResolver fallbackKeySpaceResolver;
	private TimeToLiveResolver timeToLiveResolver;

	/**
	 * Creates new {@link RedisMappingContext} with empty {@link MappingConfiguration}.
	 */
	public RedisMappingContext() {
		this(new MappingConfiguration(new IndexConfiguration(), new KeyspaceConfiguration()));
	}

	/**
	 * Creates new {@link RedisMappingContext}.
	 * 
	 * @param mappingConfiguration can be {@literal null}.
	 */
	public RedisMappingContext(MappingConfiguration mappingConfiguration) {

		this.mappingConfiguration = mappingConfiguration != null ? mappingConfiguration : new MappingConfiguration(
				new IndexConfiguration(), new KeyspaceConfiguration());

		setFallbackKeySpaceResolver(new ConfigAwareKeySpaceResolver(mappingConfiguration.getKeyspaceConfiguration()));
		this.timeToLiveResolver = new ConfigAwareTimeToLiveResolver(mappingConfiguration.getKeyspaceConfiguration());
	}

	/**
	 * Configures the {@link KeySpaceResolver} to be used if not explicit key space is annotated to the domain type.
	 * 
	 * @param fallbackKeySpaceResolver can be {@literal null}.
	 */
	public void setFallbackKeySpaceResolver(KeySpaceResolver fallbackKeySpaceResolver) {
		this.fallbackKeySpaceResolver = fallbackKeySpaceResolver;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#createPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	protected <T> RedisPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {
		return new BasicRedisPersistentEntity<T>(typeInformation, fallbackKeySpaceResolver, timeToLiveResolver);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#getPersistentEntity(java.lang.Class)
	 */
	@Override
	public RedisPersistentEntity<?> getPersistentEntity(Class<?> type) {
		return (RedisPersistentEntity<?>) super.getPersistentEntity(type);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#getPersistentEntity(org.springframework.data.mapping.PersistentProperty)
	 */
	@Override
	public RedisPersistentEntity<?> getPersistentEntity(KeyValuePersistentProperty persistentProperty) {
		return (RedisPersistentEntity<?>) super.getPersistentEntity(persistentProperty);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mapping.context.AbstractMappingContext#getPersistentEntity(org.springframework.data.util.TypeInformation)
	 */
	@Override
	public RedisPersistentEntity<?> getPersistentEntity(TypeInformation<?> type) {
		return (RedisPersistentEntity<?>) super.getPersistentEntity(type);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext#createPersistentProperty(java.lang.reflect.Field, java.beans.PropertyDescriptor, org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity, org.springframework.data.mapping.model.SimpleTypeHolder)
	 */
	@Override
	protected KeyValuePersistentProperty createPersistentProperty(Field field, PropertyDescriptor descriptor,
			KeyValuePersistentEntity<?> owner, SimpleTypeHolder simpleTypeHolder) {
		return new RedisPersistentProperty(field, descriptor, owner, simpleTypeHolder);
	}

	/**
	 * Get the {@link MappingConfiguration} used.
	 * 
	 * @return never {@literal null}.
	 */
	public MappingConfiguration getMappingConfiguration() {
		return mappingConfiguration;
	}

	/**
	 * {@link KeySpaceResolver} implementation considering {@link KeySpace} and {@link KeyspaceConfiguration}.
	 * 
	 * @author Christoph Strobl
	 */
	static class ConfigAwareKeySpaceResolver implements KeySpaceResolver {

		private final KeyspaceConfiguration keyspaceConfig;

		public ConfigAwareKeySpaceResolver(KeyspaceConfiguration keyspaceConfig) {
			this.keyspaceConfig = keyspaceConfig;
		}

		@Override
		public String resolveKeySpace(Class<?> type) {

			if (keyspaceConfig.hasSettingsFor(type)) {
				return keyspaceConfig.getKeyspaceSettings(type).getKeyspace();
			}

			return ClassNameKeySpaceResolver.INSTANCE.resolveKeySpace(type);
		}
	}

	/**
	 * {@link KeySpaceResolver} implementation considering {@link KeySpace}.
	 * 
	 * @author Christoph Strobl
	 */
	enum ClassNameKeySpaceResolver implements KeySpaceResolver {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.keyvalue.core.KeySpaceResolver#resolveKeySpace(java.lang.Class)
		 */
		@Override
		public String resolveKeySpace(Class<?> type) {

			Assert.notNull(type, "Type must not be null!");
			return ClassUtils.getUserClass(type).getName();
		}
	}

	/**
	 * {@link TimeToLiveResolver} implementation considering {@link KeyspaceConfiguration}.
	 * 
	 * @author Christoph Strobl
	 */
	static class ConfigAwareTimeToLiveResolver implements TimeToLiveResolver {

		private final KeyspaceConfiguration keyspaceConfig;

		/**
		 * Creates new {@link ConfigAwareTimeToLiveResolver}
		 * 
		 * @param keyspaceConfig must not be {@literal null}.
		 */
		public ConfigAwareTimeToLiveResolver(KeyspaceConfiguration keyspaceConfig) {

			Assert.notNull(keyspaceConfig, "KeyspaceConfiguration must not be null!");
			this.keyspaceConfig = keyspaceConfig;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.core.TimeToLiveResolver#resolveTimeToLive(java.lang.Class)
		 */
		@Override
		public Long resolveTimeToLive(Class<?> type) {

			if (keyspaceConfig.hasSettingsFor(type)) {
				return keyspaceConfig.getKeyspaceSettings(type).getTimeToLive();
			}
			return null;
		}
	}

}
