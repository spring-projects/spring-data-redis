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
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.Alternative;
import javax.enterprise.inject.Default;
import javax.enterprise.inject.Stereotype;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.InjectionPoint;
import javax.enterprise.inject.spi.PassivationCapable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Base class for {@link Bean} wrappers.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
public abstract class CdiBean<T> implements Bean<T>, PassivationCapable {

	private static final Logger LOGGER = LoggerFactory.getLogger(CdiBean.class);

	protected final BeanManager beanManager;

	private final Set<Annotation> qualifiers;
	private final Set<Type> types;
	private final Class<T> beanClass;
	private final String passivationId;

	/**
	 * Creates a new {@link CdiBean}.
	 *
	 * @param qualifiers must not be {@literal null}.
	 * @param beanClass has to be an interface must not be {@literal null}.
	 * @param beanManager the CDI {@link BeanManager}, must not be {@literal null}.
	 */
	public CdiBean(Set<Annotation> qualifiers, Class<T> beanClass, BeanManager beanManager) {
		this(qualifiers, Collections.<Type> emptySet(), beanClass, beanManager);
	}

	/**
	 * Creates a new {@link CdiBean}.
	 *
	 * @param qualifiers must not be {@literal null}.
	 * @param types additional bean types, must not be {@literal null}.
	 * @param beanClass must not be {@literal null}.
	 * @param beanManager the CDI {@link BeanManager}, must not be {@literal null}.
	 */
	public CdiBean(Set<Annotation> qualifiers, Set<Type> types, Class<T> beanClass, BeanManager beanManager) {

		Assert.notNull(qualifiers, "Qualifier annotations must not be null!");
		Assert.notNull(beanManager, "BeanManager must not be null!");
		Assert.notNull(types, "Types must not be null!");
		Assert.notNull(beanClass, "Bean class mast not be null!");

		this.qualifiers = qualifiers;
		this.types = types;
		this.beanClass = beanClass;
		this.beanManager = beanManager;
		this.passivationId = createPassivationId(qualifiers, beanClass);
	}

	/**
	 * Creates a unique identifier for the given repository type and the given annotations.
	 *
	 * @param qualifiers must not be {@literal null} or contain {@literal null} values.
	 * @param repositoryType must not be {@literal null}.
	 * @return
	 */
	private final String createPassivationId(Set<Annotation> qualifiers, Class<?> repositoryType) {

		List<String> qualifierNames = new ArrayList<>(qualifiers.size());

		for (Annotation qualifier : qualifiers) {
			qualifierNames.add(qualifier.annotationType().getName());
		}

		Collections.sort(qualifierNames);

		StringBuilder builder = new StringBuilder(StringUtils.collectionToDelimitedString(qualifierNames, ":"));
		builder.append(":").append(repositoryType.getName());

		return builder.toString();
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.Bean#getTypes()
	 */
	public Set<Type> getTypes() {

		Set<Type> types = new HashSet<>();
		types.add(beanClass);
		types.addAll(Arrays.asList(beanClass.getInterfaces()));
		types.addAll(this.types);

		return types;
	}

	/**
	 * Returns an instance of the given {@link Bean} from the container.
	 *
	 * @param <S> the actual class type of the {@link Bean}.
	 * @param bean the {@link Bean} defining the instance to create.
	 * @param type the expected component type of the instance created from the {@link Bean}.
	 * @return an instance of the given {@link Bean}.
	 * @see javax.enterprise.inject.spi.BeanManager#getReference(Bean, Type, CreationalContext)
	 * @see javax.enterprise.inject.spi.Bean
	 * @see java.lang.reflect.Type
	 */
	@SuppressWarnings("unchecked")
	protected <S> S getDependencyInstance(Bean<S> bean, Type type) {
		return (S) beanManager.getReference(bean, type, beanManager.createCreationalContext(bean));
	}

	/**
	 * Forces the initialization of bean target.
	 */
	public final void initialize() {
		create(beanManager.createCreationalContext(this));
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.context.spi.Contextual#destroy(java.lang.Object, javax.enterprise.context.spi.CreationalContext)
	 */
	public void destroy(T instance, CreationalContext<T> creationalContext) {

		if (LOGGER.isDebugEnabled()) {
			LOGGER.debug(String.format("Destroying bean instance %s for repository type '%s'.", instance.toString(),
					beanClass.getName()));
		}

		creationalContext.release();
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.Bean#getQualifiers()
	 */
	public Set<Annotation> getQualifiers() {
		return qualifiers;
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.Bean#getName()
	 */
	public String getName() {

		return getQualifiers().contains(Default.class) ? beanClass.getName()
				: beanClass.getName() + "-" + getQualifiers().toString();
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.Bean#getStereotypes()
	 */
	public Set<Class<? extends Annotation>> getStereotypes() {

		Set<Class<? extends Annotation>> stereotypes = new HashSet<>();

		for (Annotation annotation : beanClass.getAnnotations()) {
			Class<? extends Annotation> annotationType = annotation.annotationType();
			if (annotationType.isAnnotationPresent(Stereotype.class)) {
				stereotypes.add(annotationType);
			}
		}

		return stereotypes;
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.Bean#getBeanClass()
	 */
	public Class<?> getBeanClass() {
		return beanClass;
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.Bean#isAlternative()
	 */
	public boolean isAlternative() {
		return beanClass.isAnnotationPresent(Alternative.class);
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.Bean#isNullable()
	 */
	public boolean isNullable() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.Bean#getInjectionPoints()
	 */
	public Set<InjectionPoint> getInjectionPoints() {
		return Collections.emptySet();
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.Bean#getScope()
	 */
	public Class<? extends Annotation> getScope() {
		return ApplicationScoped.class;
	}

	/*
	 * (non-Javadoc)
	 * @see javax.enterprise.inject.spi.PassivationCapable#getId()
	 */
	public String getId() {
		return passivationId;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("CdiBean: type='%s', qualifiers=%s", beanClass.getName(), qualifiers.toString());
	}

}
