/*
 * Copyright 2002-2015 the original author or authors.
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
import java.util.HashSet;
import java.util.Set;

import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentEntity;
import org.springframework.data.keyvalue.core.mapping.KeyValuePersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.IndexConfiguration.RedisIndexSetting;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.util.TypeInformation;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * An {@link IndexResolver} that resolves {@link IndexedData} using a {@link SpelExpressionParser}.
 *
 * @author Rob Winch
 * @since 1.7
 */
public class SpelIndexResolver implements IndexResolver {

	private final SpelExpressionParser parser;

	private BeanResolver beanResolver;

	private final MappingContext<KeyValuePersistentEntity<?>, KeyValuePersistentProperty> mappingContext;

	private final IndexConfiguration settings;

	/**
	 * Creates a new instance using a default {@link SpelExpressionParser}.
	 *
	 * @param mappingContext the {@link RedisMappingContext} to use. Cannot be null.
	 */
	public SpelIndexResolver(RedisMappingContext mappingContext) {
		this(mappingContext, new SpelExpressionParser());
	}

	/**
	 * Creates a new instance
	 *
	 * @param mappingContext the {@link RedisMappingContext} to use. Cannot be null.
	 * @param parser the {@link SpelExpressionParser} to use. Cannot be null.
	 */
	public SpelIndexResolver(RedisMappingContext mappingContext, SpelExpressionParser parser) {
		Assert.notNull(mappingContext, "RedisMappingContext must not be null");
		Assert.notNull(parser, "SpelExpressionParser must not be null");
		this.mappingContext = mappingContext;
		this.settings = mappingContext.getMappingConfiguration().getIndexConfiguration();
		this.parser = parser;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.redis.core.convert.IndexResolver#resolveIndexesFor(org.springframework.data.util.TypeInformation, java.lang.Object)
	 */
	public Set<IndexedData> resolveIndexesFor(TypeInformation<?> typeInformation, Object value) {

		if(value == null) {
			return Collections.emptySet();
		}

		KeyValuePersistentEntity<?> entity = mappingContext.getPersistentEntity(typeInformation);

		if(entity == null) {
			return Collections.emptySet();
		}

		String keyspace = entity.getKeySpace();

		Set<IndexedData> indexes = new HashSet<IndexedData>();

		for(RedisIndexSetting setting : settings.getIndexDefinitionsFor(keyspace)) {
			Expression expression = parser.parseExpression(setting.getPath());
			StandardEvaluationContext context = new StandardEvaluationContext();
			context.setRootObject(value);
			context.setVariable("this", value);
			context.setBeanResolver(beanResolver);
			Object index = expression.getValue(context);
			if(index != null) {
				indexes.add(new SimpleIndexedPropertyValue(keyspace, setting.getIndexName(), index));
			}
		}

		return indexes;
	}

	/**
	 * Allows setting the BeanResolver
	 *
	 * @param beanResolver
	 *            the beanResolver to set
	 * @see BeanFactoryResolver
	 */
	public void setBeanResolver(BeanResolver beanResolver) {
		this.beanResolver = beanResolver;
	}
}