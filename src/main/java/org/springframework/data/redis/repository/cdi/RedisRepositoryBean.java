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
package org.springframework.data.redis.repository.cdi;

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.redis.repository.query.RedisQueryCreator;
import org.springframework.data.redis.repository.support.RedisRepositoryFactory;
import org.springframework.data.repository.cdi.CdiRepositoryBean;
import org.springframework.data.repository.config.CustomRepositoryImplementationDetector;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link CdiRepositoryBean} to create Redis repository instances.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class RedisRepositoryBean<T> extends CdiRepositoryBean<T> {

	private final Bean<KeyValueOperations> keyValueTemplate;

	/**
	 * Creates a new {@link CdiRepositoryBean}.
	 *
	 * @param keyValueTemplate must not be {@literal null}.
	 * @param qualifiers must not be {@literal null}.
	 * @param repositoryType must not be {@literal null}.
	 * @param beanManager must not be {@literal null}.
	 * @param detector detector for the custom {@link org.springframework.data.repository.Repository} implementations
	 *          {@link CustomRepositoryImplementationDetector}, can be {@literal null}.
	 */
	public RedisRepositoryBean(Bean<KeyValueOperations> keyValueTemplate, Set<Annotation> qualifiers,
			Class<T> repositoryType, BeanManager beanManager, @Nullable CustomRepositoryImplementationDetector detector) {

		super(qualifiers, repositoryType, beanManager, Optional.ofNullable(detector));

		Assert.notNull(keyValueTemplate, "Bean holding keyvalue template must not be null!");
		this.keyValueTemplate = keyValueTemplate;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.cdi.CdiRepositoryBean#create(javax.enterprise.context.spi.CreationalContext, java.lang.Class)
	 */
	@Override
	protected T create(CreationalContext<T> creationalContext, Class<T> repositoryType) {

		KeyValueOperations keyValueTemplate = getDependencyInstance(this.keyValueTemplate, KeyValueOperations.class);

		return create(() -> new RedisRepositoryFactory(keyValueTemplate, RedisQueryCreator.class), repositoryType);
	}
}
