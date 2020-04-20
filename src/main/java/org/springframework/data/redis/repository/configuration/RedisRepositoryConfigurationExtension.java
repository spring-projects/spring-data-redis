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
package org.springframework.data.redis.repository.configuration;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension;
import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueAdapter.EnableKeyspaceEvents;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.RedisCustomConversions;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.util.StringUtils;

/**
 * {@link RepositoryConfigurationExtension} for Redis.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.7
 */
public class RedisRepositoryConfigurationExtension extends KeyValueRepositoryConfigurationExtension {

	private static final String REDIS_CONVERTER_BEAN_NAME = "redisConverter";
	private static final String REDIS_REFERENCE_RESOLVER_BEAN_NAME = "redisReferenceResolver";
	private static final String REDIS_ADAPTER_BEAN_NAME = "redisKeyValueAdapter";
	private static final String REDIS_CUSTOM_CONVERSIONS_BEAN_NAME = "redisCustomConversions";

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension#getModuleName()
	 */
	@Override
	public String getModuleName() {
		return "Redis";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension#getModulePrefix()
	 */
	@Override
	protected String getModulePrefix() {
		return "redis";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension#getDefaultKeyValueTemplateRef()
	 */
	@Override
	protected String getDefaultKeyValueTemplateRef() {
		return "redisKeyValueTemplate";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension#registerBeansForRoot(org.springframework.beans.factory.support.BeanDefinitionRegistry, org.springframework.data.repository.config.RepositoryConfigurationSource)
	 */
	@Override
	public void registerBeansForRoot(BeanDefinitionRegistry registry, RepositoryConfigurationSource configuration) {

		String redisTemplateRef = configuration.getAttribute("redisTemplateRef").get();

		if (!StringUtils.hasText(redisTemplateRef)) {
			throw new IllegalStateException(
					"@EnableRedisRepositories(redisTemplateRef = … ) must be configured to a non empty value!");
		}

		registerIfNotAlreadyRegistered(() -> createRedisMappingContext(configuration), registry, MAPPING_CONTEXT_BEAN_NAME,
				configuration.getSource());

		// Register custom conversions
		registerIfNotAlreadyRegistered(() -> new RootBeanDefinition(RedisCustomConversions.class), registry,
				REDIS_CUSTOM_CONVERSIONS_BEAN_NAME, configuration.getSource());

		// Register referenceResolver
		registerIfNotAlreadyRegistered(() -> createRedisReferenceResolverDefinition(redisTemplateRef), registry,
				REDIS_REFERENCE_RESOLVER_BEAN_NAME, configuration.getSource());

		// Register converter
		registerIfNotAlreadyRegistered(() -> createRedisConverterDefinition(), registry, REDIS_CONVERTER_BEAN_NAME,
				configuration.getSource());

		registerIfNotAlreadyRegistered(() -> createRedisKeyValueAdapter(configuration), registry, REDIS_ADAPTER_BEAN_NAME,
				configuration.getSource());

		super.registerBeansForRoot(registry, configuration);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension#getDefaultKeyValueTemplateBeanDefinition(org.springframework.data.repository.config.RepositoryConfigurationSource)
	 */
	@Override
	protected AbstractBeanDefinition getDefaultKeyValueTemplateBeanDefinition(
			RepositoryConfigurationSource configurationSource) {

		return BeanDefinitionBuilder.rootBeanDefinition(RedisKeyValueTemplate.class) //
				.addConstructorArgReference(REDIS_ADAPTER_BEAN_NAME) //
				.addConstructorArgReference(MAPPING_CONTEXT_BEAN_NAME) //
				.getBeanDefinition();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#getIdentifyingAnnotations()
	 */
	@Override
	protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
		return Collections.singleton(RedisHash.class);
	}

	private static AbstractBeanDefinition createRedisKeyValueAdapter(RepositoryConfigurationSource configuration) {

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(RedisKeyValueAdapter.class) //
				.addConstructorArgReference(configuration.getRequiredAttribute("redisTemplateRef", String.class)) //
				.addConstructorArgReference(REDIS_CONVERTER_BEAN_NAME);

		if (configuration.getSource() instanceof AnnotatedTypeMetadata) {

			MergedAnnotation<EnableKeyspaceNotifications> enableAnnotation = ((AnnotatedTypeMetadata) configuration
					.getSource()).getAnnotations().get(EnableKeyspaceNotifications.class);

			if (enableAnnotation.isPresent()) {

				return builder
						.addPropertyValue("enableKeyspaceEvents",
								enableAnnotation.getValue("enabled", EnableKeyspaceEvents.class).get()) //
						.addPropertyValue("keyspaceNotificationsConfigParameter",
								enableAnnotation.getValue("notifyKeyspaceEvents").orElse(""))
						.addPropertyValue("keyspaceNotificationsDatabase", enableAnnotation.getValue("database").get())
						.getBeanDefinition();
			}
		}

		return builder
				.addPropertyValue("enableKeyspaceEvents",
						configuration.getRequiredAttribute("enableKeyspaceEvents", EnableKeyspaceEvents.class)) //
				.addPropertyValue("keyspaceNotificationsConfigParameter",
						configuration.getAttribute("keyspaceNotificationsConfigParameter", String.class).orElse(""))
				.getBeanDefinition();
	}

	private static AbstractBeanDefinition createRedisReferenceResolverDefinition(String redisTemplateRef) {

		return BeanDefinitionBuilder.rootBeanDefinition("org.springframework.data.redis.core.convert.ReferenceResolverImpl") //
				.addConstructorArgReference(redisTemplateRef) //
				.getBeanDefinition();
	}

	private static AbstractBeanDefinition createRedisMappingContext(RepositoryConfigurationSource configurationSource) {

		return BeanDefinitionBuilder.rootBeanDefinition(RedisMappingContext.class) //
				.addConstructorArgValue(createMappingConfigBeanDef(configurationSource)) //
				.getBeanDefinition();
	}

	private static BeanDefinition createMappingConfigBeanDef(RepositoryConfigurationSource configuration) {

		BeanDefinition indexDefinition = BeanDefinitionBuilder
				.genericBeanDefinition(configuration.getRequiredAttribute("indexConfiguration", Class.class)) //
				.getBeanDefinition();

		BeanDefinition keyspaceDefinition = BeanDefinitionBuilder
				.genericBeanDefinition(configuration.getRequiredAttribute("keyspaceConfiguration", Class.class)) //
				.getBeanDefinition();

		return BeanDefinitionBuilder.genericBeanDefinition(MappingConfiguration.class) //
				.addConstructorArgValue(indexDefinition) //
				.addConstructorArgValue(keyspaceDefinition) //
				.getBeanDefinition();
	}

	private static AbstractBeanDefinition createRedisConverterDefinition() {

		return BeanDefinitionBuilder.rootBeanDefinition(MappingRedisConverter.class) //
				.addConstructorArgReference(MAPPING_CONTEXT_BEAN_NAME) //
				.addPropertyReference("referenceResolver", REDIS_REFERENCE_RESOLVER_BEAN_NAME) //
				.addPropertyReference("customConversions", REDIS_CUSTOM_CONVERSIONS_BEAN_NAME) //
				.getBeanDefinition();
	}
}
