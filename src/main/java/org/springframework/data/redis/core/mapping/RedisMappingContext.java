/*
 * Copyright 2015-2023 the original author or authors.
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
package org.springframework.data.redis.core.mapping;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.keyvalue.core.mapping.KeySpaceResolver;
import org.springframework.data.keyvalue.core.mapping.context.KeyValueMappingContext;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.redis.core.PartialUpdate;
import org.springframework.data.redis.core.PartialUpdate.PropertyUpdate;
import org.springframework.data.redis.core.PartialUpdate.UpdateCommand;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;
import org.springframework.data.redis.core.TimeToLiveAccessor;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration.KeyspaceSettings;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.NumberUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;

/**
 * Redis specific {@link MappingContext}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisMappingContext extends KeyValueMappingContext<RedisPersistentEntity<?>, RedisPersistentProperty> {

	private static final SimpleTypeHolder SIMPLE_TYPE_HOLDER = new RedisCustomConversions().getSimpleTypeHolder();

	private final MappingConfiguration mappingConfiguration;
	private final TimeToLiveAccessor timeToLiveAccessor;

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
	public RedisMappingContext(@Nullable MappingConfiguration mappingConfiguration) {

		this.mappingConfiguration = mappingConfiguration != null ? mappingConfiguration
				: new MappingConfiguration(new IndexConfiguration(), new KeyspaceConfiguration());

		setKeySpaceResolver(new ConfigAwareKeySpaceResolver(this.mappingConfiguration.getKeyspaceConfiguration()));
		this.timeToLiveAccessor = new ConfigAwareTimeToLiveAccessor(this.mappingConfiguration.getKeyspaceConfiguration(),
				this);
		this.setSimpleTypeHolder(SIMPLE_TYPE_HOLDER);
	}

	@Override
	protected <T> RedisPersistentEntity<T> createPersistentEntity(TypeInformation<T> typeInformation) {
		return new BasicRedisPersistentEntity<>(typeInformation, getKeySpaceResolver(), timeToLiveAccessor);
	}

	@Override
	protected RedisPersistentProperty createPersistentProperty(Property property, RedisPersistentEntity<?> owner,
			SimpleTypeHolder simpleTypeHolder) {
		return new RedisPersistentProperty(property, owner, simpleTypeHolder);
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

		@Override
		public String resolveKeySpace(Class<?> type) {

			Assert.notNull(type, "Type must not be null");
			if (keyspaceConfig.hasSettingsFor(type)) {

				String value = keyspaceConfig.getKeyspaceSettings(type).getKeyspace();
				if (StringUtils.hasText(value)) {
					return value;
				}
			}

			return null;
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

		@Override
		public String resolveKeySpace(Class<?> type) {

			Assert.notNull(type, "Type must not be null");
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

			Assert.notNull(keyspaceConfig, "KeyspaceConfiguration must not be null");
			Assert.notNull(mappingContext, "MappingContext must not be null");

			this.defaultTimeouts = new HashMap<>();
			this.timeoutProperties = new HashMap<>();
			this.timeoutMethods = new HashMap<>();
			this.keyspaceConfig = keyspaceConfig;
			this.mappingContext = mappingContext;
		}

		@Override
		@SuppressWarnings({ "rawtypes" })
		public Long getTimeToLive(Object source) {

			Assert.notNull(source, "Source must not be null");
			Class<?> type = source instanceof Class<?> ? (Class<?>) source
					: (source instanceof PartialUpdate ? ((PartialUpdate) source).getTarget() : source.getClass());

			Long defaultTimeout = resolveDefaultTimeOut(type);
			TimeUnit unit = TimeUnit.SECONDS;

			PersistentProperty<?> ttlProperty = resolveTtlProperty(type);

			if (ttlProperty != null && ttlProperty.isAnnotationPresent(TimeToLive.class)) {
				unit = ttlProperty.getRequiredAnnotation(TimeToLive.class).unit();
			}

			if (source instanceof PartialUpdate<?> update) {

				if (ttlProperty != null && !update.getPropertyUpdates().isEmpty()) {
					for (PropertyUpdate pUpdate : update.getPropertyUpdates()) {

						if (UpdateCommand.SET.equals(pUpdate.getCmd()) && ttlProperty.getName().equals(pUpdate.getPropertyPath())) {

							return TimeUnit.SECONDS
									.convert(NumberUtils.convertNumberToTargetClass((Number) pUpdate.getValue(), Long.class), unit);
						}
					}
				}

			} else if (ttlProperty != null) {

				RedisPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);

				Object ttlPropertyValue = entity.getPropertyAccessor(source).getProperty(ttlProperty);
				if (ttlPropertyValue != null) {
					return TimeUnit.SECONDS.convert(((Number) ttlPropertyValue).longValue(), unit);
				}

			} else {

				Method timeoutMethod = resolveTimeMethod(type);

				if (timeoutMethod != null) {

					ReflectionUtils.makeAccessible(timeoutMethod);

					TimeToLive ttl = AnnotationUtils.findAnnotation(timeoutMethod, TimeToLive.class);
					try {
						Number timeout = (Number) timeoutMethod.invoke(source);
						if (timeout != null && ttl != null) {
							return TimeUnit.SECONDS.convert(timeout.longValue(), ttl.unit());
						}
					} catch (IllegalAccessException ex) {
						String message = String.format("Not allowed to access method '%s': %s",
								timeoutMethod.getName(), ex.getMessage());
						throw new IllegalStateException(message, ex);
					} catch (IllegalArgumentException ex) {
						String message = String.format("Cannot invoke method '%s' without arguments: %s",
								timeoutMethod.getName(), ex.getMessage());
						throw new IllegalStateException(message, ex);
					} catch (InvocationTargetException ex) {
						String message = String.format("Cannot access method '%s': %s",
								timeoutMethod.getName(), ex.getMessage());
						throw new IllegalStateException(message, ex);
					}
				}
			}

			return defaultTimeout;
		}

		@Override
		public boolean isExpiringEntity(Class<?> type) {

			Long defaultTimeOut = resolveDefaultTimeOut(type);

			if (defaultTimeOut != null && defaultTimeOut > 0) {
				return true;
			}

			if (resolveTtlProperty(type) != null) {
				return true;
			}

			return resolveTimeMethod(type) != null;
		}

		@Nullable
		private Long resolveDefaultTimeOut(Class<?> type) {

			if (this.defaultTimeouts.containsKey(type)) {
				return defaultTimeouts.get(type);
			}

			Long defaultTimeout = null;

			if (keyspaceConfig.hasSettingsFor(type)) {
				defaultTimeout = keyspaceConfig.getKeyspaceSettings(type).getTimeToLive();
			}

			RedisHash hash = mappingContext.getRequiredPersistentEntity(type).findAnnotation(RedisHash.class);
			if (hash != null && hash.timeToLive() > 0) {
				defaultTimeout = hash.timeToLive();
			}

			defaultTimeouts.put(type, defaultTimeout);
			return defaultTimeout;
		}

		@Nullable
		private PersistentProperty<?> resolveTtlProperty(Class<?> type) {

			if (timeoutProperties.containsKey(type)) {
				return timeoutProperties.get(type);
			}

			RedisPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);
			PersistentProperty<?> ttlProperty = entity.getPersistentProperty(TimeToLive.class);

			if (ttlProperty != null) {

				timeoutProperties.put(type, ttlProperty);
				return ttlProperty;
			}

			if (keyspaceConfig.hasSettingsFor(type)) {

				KeyspaceSettings settings = keyspaceConfig.getKeyspaceSettings(type);
				if (StringUtils.hasText(settings.getTimeToLivePropertyName())) {

					ttlProperty = entity.getPersistentProperty(settings.getTimeToLivePropertyName());
					if (ttlProperty != null) {
						timeoutProperties.put(type, ttlProperty);
						return ttlProperty;
					}
				}
			}

			timeoutProperties.put(type, null);
			return null;
		}

		@Nullable
		private Method resolveTimeMethod(Class<?> type) {

			if (timeoutMethods.containsKey(type)) {
				return timeoutMethods.get(type);
			}

			timeoutMethods.put(type, null);
			ReflectionUtils.doWithMethods(type, method -> timeoutMethods.put(type, method),
					method -> ClassUtils.isAssignable(Number.class, method.getReturnType())
							&& AnnotationUtils.findAnnotation(method, TimeToLive.class) != null);

			return timeoutMethods.get(type);
		}
	}

}
