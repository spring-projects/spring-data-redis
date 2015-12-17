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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.core.mapping.KeySpaceResolver;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;
import org.springframework.data.redis.core.TimeToLiveAccessor;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration.KeyspaceSettings;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.MethodCallback;
import org.springframework.util.ReflectionUtils.MethodFilter;
import org.springframework.util.StringUtils;

/**
 * Redis specific {@link MappingContext}.
 * 
 * @author Christoph Strobl
 * @since 1.7
 */
public class RedisMappingContext extends KeyValueMappingContext {

	private MappingConfiguration mappingConfiguration;
	private KeySpaceResolver fallbackKeySpaceResolver;
	private TimeToLiveAccessor timeToLiveAccessor;

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

		setFallbackKeySpaceResolver(new ConfigAwareKeySpaceResolver(this.mappingConfiguration.getKeyspaceConfiguration()));
		this.timeToLiveAccessor = new ConfigAwareTimeToLiveAccessor(this.mappingConfiguration.getKeyspaceConfiguration(),
				this);
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
		return new BasicRedisPersistentEntity<T>(typeInformation, fallbackKeySpaceResolver, timeToLiveAccessor);
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
	 * @since 1.7
	 */
	static class ConfigAwareKeySpaceResolver implements KeySpaceResolver {

		private final KeyspaceConfiguration keyspaceConfig;

		public ConfigAwareKeySpaceResolver(KeyspaceConfiguration keyspaceConfig) {

			this.keyspaceConfig = keyspaceConfig;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.keyvalue.core.mapping.KeySpaceResolver#resolveKeySpace(java.lang.Class)
		 */
		@Override
		public String resolveKeySpace(Class<?> type) {

			Assert.notNull(type, "Type must not be null!");
			if (keyspaceConfig.hasSettingsFor(type)) {

				String value = keyspaceConfig.getKeyspaceSettings(type).getKeyspace();
				if (StringUtils.hasText(value)) {
					return value;
				}
			}

			return ClassNameKeySpaceResolver.INSTANCE.resolveKeySpace(type);
		}
	}

	/**
	 * {@link KeySpaceResolver} implementation considering {@link KeySpace}.
	 * 
	 * @author Christoph Strobl
	 * @since 1.7
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
	 * {@link TimeToLiveAccessor} implementation considering {@link KeyspaceConfiguration}.
	 * 
	 * @author Christoph Strobl
	 * @since 1.7
	 */
	static class ConfigAwareTimeToLiveAccessor implements TimeToLiveAccessor {

		private final Map<Class<?>, Long> defaultTimeouts;
		private final Map<Class<?>, PersistentProperty<?>> timeoutProperties;
		private final Map<Class<?>, Method> timeoutMethods;
		private final KeyspaceConfiguration keyspaceConfig;
		private final RedisMappingContext mappingContext;

		/**
		 * Creates new {@link ConfigAwareTimeToLiveAccessor}
		 * 
		 * @param keyspaceConfig must not be {@literal null}.
		 * @param mappingContext must not be {@literal null}.
		 */
		ConfigAwareTimeToLiveAccessor(KeyspaceConfiguration keyspaceConfig, RedisMappingContext mappingContext) {

			Assert.notNull(keyspaceConfig, "KeyspaceConfiguration must not be null!");
			Assert.notNull(mappingContext, "MappingContext must not be null!");

			this.defaultTimeouts = new HashMap<Class<?>, Long>();
			this.timeoutProperties = new HashMap<Class<?>, PersistentProperty<?>>();
			this.timeoutMethods = new HashMap<Class<?>, Method>();
			this.keyspaceConfig = keyspaceConfig;
			this.mappingContext = mappingContext;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.core.TimeToLiveResolver#resolveTimeToLive(java.lang.Object)
		 */
		@Override
		@SuppressWarnings({ "rawtypes" })
		public Long getTimeToLive(final Object source) {

			Assert.notNull(source, "Source must not be null!");
			Class<?> type = source instanceof Class<?> ? (Class<?>) source : source.getClass();

			Long defaultTimeout = resolveDefaultTimeOut(type);
			TimeUnit unit = TimeUnit.SECONDS;

			PersistentProperty<?> ttlProperty = resolveTtlProperty(type);

			if (ttlProperty != null) {

				if (ttlProperty.findAnnotation(TimeToLive.class) != null) {
					unit = ttlProperty.findAnnotation(TimeToLive.class).unit();
				}

				RedisPersistentEntity entity = mappingContext.getPersistentEntity(type);
				Long timeout = (Long) entity.getPropertyAccessor(source).getProperty(ttlProperty);
				if (timeout != null) {
					return TimeUnit.SECONDS.convert(timeout, unit);
				}
			} else {

				Method timeoutMethod = resolveTimeMethod(type);
				if (timeoutMethod != null) {

					TimeToLive ttl = AnnotationUtils.findAnnotation(timeoutMethod, TimeToLive.class);
					try {
						Number timeout = (Number) timeoutMethod.invoke(source);
						if (timeout != null) {
							return TimeUnit.SECONDS.convert(timeout.longValue(), ttl.unit());
						}
					} catch (IllegalAccessException e) {
						throw new IllegalStateException("Not allowed to access method '" + timeoutMethod.getName() + "': "
								+ e.getMessage(), e);
					} catch (IllegalArgumentException e) {
						throw new IllegalStateException("Cannot invoke method '" + timeoutMethod.getName()
								+ " without arguments': " + e.getMessage(), e);
					} catch (InvocationTargetException e) {
						throw new IllegalStateException(
								"Cannot access method '" + timeoutMethod.getName() + "': " + e.getMessage(), e);
					}
				}
			}

			return defaultTimeout;
		}

		private Long resolveDefaultTimeOut(Class<?> type) {

			if (this.defaultTimeouts.containsKey(type)) {
				return defaultTimeouts.get(type);
			}

			Long defaultTimeout = null;

			if (keyspaceConfig.hasSettingsFor(type)) {
				defaultTimeout = keyspaceConfig.getKeyspaceSettings(type).getTimeToLive();
			}

			RedisHash hash = mappingContext.getPersistentEntity(type).findAnnotation(RedisHash.class);
			if (hash != null && hash.timeToLive() > 0) {
				defaultTimeout = hash.timeToLive();
			}

			defaultTimeouts.put(type, defaultTimeout);
			return defaultTimeout;
		}

		@SuppressWarnings({ "rawtypes", "unchecked" })
		private PersistentProperty<?> resolveTtlProperty(Class<?> type) {

			if (timeoutProperties.containsKey(type)) {
				return timeoutProperties.get(type);
			}

			RedisPersistentEntity entity = mappingContext.getPersistentEntity(type);
			PersistentProperty<?> ttlProperty = entity.getPersistentProperty(TimeToLive.class);

			if (ttlProperty != null) {

				timeoutProperties.put(type, ttlProperty);
				return ttlProperty;
			}

			if (keyspaceConfig.hasSettingsFor(type)) {

				KeyspaceSettings settings = keyspaceConfig.getKeyspaceSettings(type);
				if (StringUtils.hasText(settings.getTimeToLivePropertyName())) {

					ttlProperty = entity.getPersistentProperty(settings.getTimeToLivePropertyName());
					timeoutProperties.put(type, ttlProperty);
					return ttlProperty;
				}
			}

			timeoutProperties.put(type, null);
			return null;
		}

		private Method resolveTimeMethod(final Class<?> type) {

			if (timeoutMethods.containsKey(type)) {
				return timeoutMethods.get(type);
			}

			timeoutMethods.put(type, null);
			ReflectionUtils.doWithMethods(type, new MethodCallback() {

				@Override
				public void doWith(Method method) throws IllegalArgumentException, IllegalAccessException {
					timeoutMethods.put(type, method);
				}
			}, new MethodFilter() {

				@Override
				public boolean matches(Method method) {
					return ClassUtils.isAssignable(Number.class, method.getReturnType())
							&& AnnotationUtils.findAnnotation(method, TimeToLive.class) != null;
				}
			});

			return timeoutMethods.get(type);
		}
	}

}
