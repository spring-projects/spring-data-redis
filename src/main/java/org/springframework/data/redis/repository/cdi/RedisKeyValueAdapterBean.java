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
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.util.Assert;

/**
 * {@link CdiBean} to create {@link RedisKeyValueAdapter} instances.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public class RedisKeyValueAdapterBean extends CdiBean<RedisKeyValueAdapter> {

	private final Bean<RedisOperations<?, ?>> redisOperations;

	/**
	 * Creates a new {@link RedisKeyValueAdapterBean}.
	 *
	 * @param redisOperations must not be {@literal null}.
	 * @param qualifiers must not be {@literal null}.
	 * @param beanManager must not be {@literal null}.
	 */
	public RedisKeyValueAdapterBean(Bean<RedisOperations<?, ?>> redisOperations, Set<Annotation> qualifiers,
			BeanManager beanManager) {

		super(qualifiers, RedisKeyValueAdapter.class, beanManager);
		Assert.notNull(redisOperations, "RedisOperations Bean must not be null!");
		this.redisOperations = redisOperations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.cdi.CdiRepositoryBean#create(javax.enterprise.context.spi.CreationalContext, java.lang.Class)
	 */
	@Override
	public RedisKeyValueAdapter create(CreationalContext<RedisKeyValueAdapter> creationalContext) {

		Type beanType = getBeanType();

		return new RedisKeyValueAdapter(getDependencyInstance(this.redisOperations, beanType));
	}

	private Type getBeanType() {

		for (Type type : this.redisOperations.getTypes()) {
			if (type instanceof Class<?> && RedisOperations.class.isAssignableFrom((Class<?>) type)) {
				return type;
			}

			if (type instanceof ParameterizedType) {
				ParameterizedType parameterizedType = (ParameterizedType) type;
				if (parameterizedType.getRawType() instanceof Class<?>
						&& RedisOperations.class.isAssignableFrom((Class<?>) parameterizedType.getRawType())) {
					return type;
				}
			}
		}
		throw new IllegalStateException("Cannot resolve bean type for class " + RedisOperations.class.getName());
	}

	@Override
	public void destroy(RedisKeyValueAdapter instance, CreationalContext<RedisKeyValueAdapter> creationalContext) {

		if (instance instanceof DisposableBean) {
			try {
				instance.destroy();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}

		super.destroy(instance, creationalContext);
	}

}
