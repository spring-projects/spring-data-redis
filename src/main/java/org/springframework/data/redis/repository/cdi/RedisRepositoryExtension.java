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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.UnsatisfiedResolutionException;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.ProcessBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.redis.core.RedisKeyValueAdapter;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.repository.cdi.CdiRepositoryBean;
import org.springframework.data.repository.cdi.CdiRepositoryExtensionSupport;

/**
 * CDI extension to export Redis repositories. This extension enables Redis
 * {@link org.springframework.data.repository.Repository} support. It requires either a {@link RedisKeyValueTemplate} or a
 * {@link RedisOperations} bean. If no {@link RedisKeyValueTemplate} or {@link RedisKeyValueAdapter} are provided by the
 * user, the extension creates own managed beans.
 *
 * @author Mark Paluch
 */
public class RedisRepositoryExtension extends CdiRepositoryExtensionSupport {

	private static final Logger LOG = LoggerFactory.getLogger(RedisRepositoryExtension.class);

	private final Map<Set<Annotation>, Bean<RedisKeyValueAdapter>> redisKeyValueAdapters = new HashMap<>();
	private final Map<Set<Annotation>, Bean<KeyValueOperations>> redisKeyValueTemplates = new HashMap<>();
	private final Map<Set<Annotation>, Bean<RedisOperations<?, ?>>> redisOperations = new HashMap<>();

	public RedisRepositoryExtension() {
		LOG.info("Activating CDI extension for Spring Data Redis repositories.");
	}

	/**
	 * Pick up existing bean definitions that are required for a Repository to work.
	 *
	 * @param processBean
	 * @param <X>
	 */
	@SuppressWarnings("unchecked")
	<X> void processBean(@Observes ProcessBean<X> processBean) {

		Bean<X> bean = processBean.getBean();

		for (Type type : bean.getTypes()) {
			Type beanType = type;

			if (beanType instanceof ParameterizedType) {
				beanType = ((ParameterizedType) beanType).getRawType();
			}

			if (beanType instanceof Class<?> && RedisKeyValueTemplate.class.isAssignableFrom((Class<?>) beanType)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Discovered %s with qualifiers %s.", RedisKeyValueTemplate.class.getName(),
							bean.getQualifiers()));
				}

				// Store the Key-Value Templates bean using its qualifiers.
				redisKeyValueTemplates.put(new HashSet<>(bean.getQualifiers()), (Bean<KeyValueOperations>) bean);
			}

			if (beanType instanceof Class<?> && RedisKeyValueAdapter.class.isAssignableFrom((Class<?>) beanType)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Discovered %s with qualifiers %s.", RedisKeyValueAdapter.class.getName(),
							bean.getQualifiers()));
				}

				// Store the RedisKeyValueAdapter bean using its qualifiers.
				redisKeyValueAdapters.put(new HashSet<>(bean.getQualifiers()), (Bean<RedisKeyValueAdapter>) bean);
			}

			if (beanType instanceof Class<?> && RedisOperations.class.isAssignableFrom((Class<?>) beanType)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Discovered %s with qualifiers %s.", RedisOperations.class.getName(),
							bean.getQualifiers()));
				}

				// Store the RedisOperations bean using its qualifiers.
				redisOperations.put(new HashSet<>(bean.getQualifiers()), (Bean<RedisOperations<?, ?>>) bean);
			}
		}
	}

	void afterBeanDiscovery(@Observes AfterBeanDiscovery afterBeanDiscovery, BeanManager beanManager) {

		registerDependenciesIfNecessary(afterBeanDiscovery, beanManager);

		for (Entry<Class<?>, Set<Annotation>> entry : getRepositoryTypes()) {

			Class<?> repositoryType = entry.getKey();
			Set<Annotation> qualifiers = entry.getValue();

			// Create the bean representing the repository.
			CdiRepositoryBean<?> repositoryBean = createRepositoryBean(repositoryType, qualifiers, beanManager);

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Registering bean for %s with qualifiers %s.", repositoryType.getName(), qualifiers));
			}

			// Register the bean to the container.
			registerBean(repositoryBean);
			afterBeanDiscovery.addBean(repositoryBean);
		}
	}

	/**
	 * Register {@link RedisKeyValueAdapter} and {@link RedisKeyValueTemplate} if these beans are not provided by the CDI
	 * application.
	 *
	 * @param afterBeanDiscovery
	 * @param beanManager
	 */
	private void registerDependenciesIfNecessary(@Observes AfterBeanDiscovery afterBeanDiscovery,
			BeanManager beanManager) {

		for (Entry<Class<?>, Set<Annotation>> entry : getRepositoryTypes()) {

			Set<Annotation> qualifiers = entry.getValue();

			if (!redisKeyValueAdapters.containsKey(qualifiers)) {
				if (LOG.isInfoEnabled()) {
					LOG.info(String.format("Registering bean for %s with qualifiers %s.", RedisKeyValueAdapter.class.getName(),
							qualifiers));
				}
				RedisKeyValueAdapterBean redisKeyValueAdapterBean = createRedisKeyValueAdapterBean(qualifiers, beanManager);
				redisKeyValueAdapters.put(qualifiers, redisKeyValueAdapterBean);
				afterBeanDiscovery.addBean(redisKeyValueAdapterBean);
			}

			if (!redisKeyValueTemplates.containsKey(qualifiers)) {
				if (LOG.isInfoEnabled()) {
					LOG.info(String.format("Registering bean for %s with qualifiers %s.", RedisKeyValueTemplate.class.getName(),
							qualifiers));
				}

				RedisKeyValueTemplateBean redisKeyValueTemplateBean = createRedisKeyValueTemplateBean(qualifiers, beanManager);
				redisKeyValueTemplates.put(qualifiers, redisKeyValueTemplateBean);
				afterBeanDiscovery.addBean(redisKeyValueTemplateBean);
			}
		}
	}

	/**
	 * Creates a {@link CdiRepositoryBean} for the repository of the given type, requires a {@link KeyValueOperations}
	 * bean with the same qualifiers.
	 *
	 * @param <T> the type of the repository.
	 * @param repositoryType the class representing the repository.
	 * @param qualifiers the qualifiers to be applied to the bean.
	 * @param beanManager the BeanManager instance.
	 * @return
	 */
	private <T> CdiRepositoryBean<T> createRepositoryBean(Class<T> repositoryType, Set<Annotation> qualifiers,
			BeanManager beanManager) {

		// Determine the KeyValueOperations bean which matches the qualifiers of the repository.
		Bean<KeyValueOperations> redisKeyValueTemplate = this.redisKeyValueTemplates.get(qualifiers);

		if (redisKeyValueTemplate == null) {
			throw new UnsatisfiedResolutionException(String.format("Unable to resolve a bean for '%s' with qualifiers %s.",
					RedisKeyValueTemplate.class.getName(), qualifiers));
		}

		// Construct and return the repository bean.
		return new RedisRepositoryBean<>(redisKeyValueTemplate, qualifiers, repositoryType, beanManager,
				getCustomImplementationDetector());
	}

	/**
	 * Creates a {@link RedisKeyValueAdapterBean}, requires a {@link RedisOperations} bean with the same qualifiers.
	 *
	 * @param qualifiers the qualifiers to be applied to the bean.
	 * @param beanManager the BeanManager instance.
	 * @return
	 */
	private RedisKeyValueAdapterBean createRedisKeyValueAdapterBean(Set<Annotation> qualifiers, BeanManager beanManager) {

		// Determine the RedisOperations bean which matches the qualifiers of the repository.
		Bean<RedisOperations<?, ?>> redisOperationsBean = this.redisOperations.get(qualifiers);

		if (redisOperationsBean == null) {
			throw new UnsatisfiedResolutionException(String.format("Unable to resolve a bean for '%s' with qualifiers %s.",
					RedisOperations.class.getName(), qualifiers));
		}

		// Construct and return the repository bean.
		return new RedisKeyValueAdapterBean(redisOperationsBean, qualifiers, beanManager);
	}

	/**
	 * Creates a {@link RedisKeyValueTemplateBean}, requires a {@link RedisKeyValueAdapter} bean with the same qualifiers.
	 *
	 * @param qualifiers the qualifiers to be applied to the bean.
	 * @param beanManager the BeanManager instance.
	 * @return
	 */
	private RedisKeyValueTemplateBean createRedisKeyValueTemplateBean(Set<Annotation> qualifiers,
			BeanManager beanManager) {

		// Determine the RedisKeyValueAdapter bean which matches the qualifiers of the repository.
		Bean<RedisKeyValueAdapter> redisKeyValueAdapterBean = this.redisKeyValueAdapters.get(qualifiers);

		if (redisKeyValueAdapterBean == null) {
			throw new UnsatisfiedResolutionException(String.format("Unable to resolve a bean for '%s' with qualifiers %s.",
					RedisKeyValueAdapter.class.getName(), qualifiers));
		}

		// Construct and return the repository bean.
		return new RedisKeyValueTemplateBean(redisKeyValueAdapterBean, qualifiers, beanManager);
	}

}
