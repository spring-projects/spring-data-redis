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
package org.springframework.data.redis.repository.configuration;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConstructorArgumentValues;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 */
public class RedisRepositoryConfigurationExtension extends KeyValueRepositoryConfigurationExtension {

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

	@Override
	public void registerBeansForRoot(BeanDefinitionRegistry registry, RepositoryConfigurationSource configurationSource) {

		RootBeanDefinition mappingContextDefinition = createRedisMappingContext(configurationSource);
		mappingContextDefinition.setSource(configurationSource.getSource());

		registerIfNotAlreadyRegistered(mappingContextDefinition, registry, MAPPING_CONTEXT_BEAN_NAME, configurationSource);

		super.registerBeansForRoot(registry, configurationSource);
	}

	private RootBeanDefinition createRedisMappingContext(RepositoryConfigurationSource configurationSource) {

		ConstructorArgumentValues mappingContextArgs = new ConstructorArgumentValues();
		mappingContextArgs.addIndexedArgumentValue(0, createMappingConfigBeanDef(configurationSource));

		RootBeanDefinition mappingContextBeanDef = new RootBeanDefinition(RedisMappingContext.class);
		mappingContextBeanDef.setConstructorArgumentValues(mappingContextArgs);

		return mappingContextBeanDef;
	}

	private BeanDefinition createMappingConfigBeanDef(RepositoryConfigurationSource configurationSource) {

		DirectFieldAccessor dfa = new DirectFieldAccessor(configurationSource);
		AnnotationAttributes aa = (AnnotationAttributes) dfa.getPropertyValue("attributes");

		GenericBeanDefinition indexConfiguration = new GenericBeanDefinition();
		indexConfiguration.setBeanClass(aa.getClass("indexConfiguration"));

		GenericBeanDefinition keyspaceConfig = new GenericBeanDefinition();
		keyspaceConfig.setBeanClass(aa.getClass("keyspaceConfiguration"));

		ConstructorArgumentValues mappingConfigArgs = new ConstructorArgumentValues();
		mappingConfigArgs.addIndexedArgumentValue(0, indexConfiguration);
		mappingConfigArgs.addIndexedArgumentValue(1, keyspaceConfig);

		GenericBeanDefinition mappingConfigBeanDef = new GenericBeanDefinition();
		mappingConfigBeanDef.setBeanClass(MappingConfiguration.class);
		mappingConfigBeanDef.setConstructorArgumentValues(mappingConfigArgs);

		return mappingConfigBeanDef;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.keyvalue.repository.config.KeyValueRepositoryConfigurationExtension#getDefaultKeyValueTemplateBeanDefinition(org.springframework.data.repository.config.RepositoryConfigurationSource)
	 */
	@Override
	protected AbstractBeanDefinition getDefaultKeyValueTemplateBeanDefinition(
			RepositoryConfigurationSource configurationSource) {

		RootBeanDefinition redisKeyValueAdapterDefinition = new RootBeanDefinition(RedisKeyValueAdapter.class);

		ConstructorArgumentValues constructorArgumentValuesForRedisKeyValueAdapter = new ConstructorArgumentValues();

		String redisTemplateRef = configurationSource.getAttribute("redisTemplateRef");
		if (StringUtils.hasText(redisTemplateRef)) {

			constructorArgumentValuesForRedisKeyValueAdapter.addIndexedArgumentValue(0, new RuntimeBeanReference(
					redisTemplateRef));
		}

		constructorArgumentValuesForRedisKeyValueAdapter.addIndexedArgumentValue(1, new RuntimeBeanReference(
				MAPPING_CONTEXT_BEAN_NAME));
		redisKeyValueAdapterDefinition.setConstructorArgumentValues(constructorArgumentValuesForRedisKeyValueAdapter);

		RootBeanDefinition keyValueTemplateDefinition = new RootBeanDefinition(RedisKeyValueTemplate.class);

		ConstructorArgumentValues constructorArgumentValuesForKeyValueTemplate = new ConstructorArgumentValues();
		constructorArgumentValuesForKeyValueTemplate.addIndexedArgumentValue(0, redisKeyValueAdapterDefinition);
		constructorArgumentValuesForKeyValueTemplate.addIndexedArgumentValue(1, new RuntimeBeanReference(
				MAPPING_CONTEXT_BEAN_NAME));

		keyValueTemplateDefinition.setConstructorArgumentValues(constructorArgumentValuesForKeyValueTemplate);

		return keyValueTemplateDefinition;
	}

}
