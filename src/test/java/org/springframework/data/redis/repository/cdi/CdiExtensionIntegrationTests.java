/*
 * Copyright 2016 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.spi.Bean;

import org.apache.webbeans.cditest.CdiTestContainer;
import org.apache.webbeans.cditest.CdiTestContainerLoader;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for Spring Data Redis CDI extension.
 *
 * @author Mark Paluch
 */
public class CdiExtensionIntegrationTests {

	private static Logger LOGGER = LoggerFactory.getLogger(CdiExtensionIntegrationTests.class);

	static CdiTestContainer container;

	@BeforeClass
	public static void setUp() throws Exception {

		container = CdiTestContainerLoader.getCdiContainer();
		container.bootContainer();

		LOGGER.debug("CDI container bootstrapped!");
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	@SuppressWarnings("rawtypes")
	public void beanShouldBeRegistered() {

		Set<Bean<?>> beans = container.getBeanManager().getBeans(PersonRepository.class);

		assertThat(beans, hasSize(1));
		assertThat(beans.iterator().next().getScope(), is(equalTo((Class) ApplicationScoped.class)));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void saveAndFindUnqualified() {

		RepositoryConsumer repositoryConsumer = container.getInstance(RepositoryConsumer.class);
		repositoryConsumer.deleteAll();

		Person person = new Person();
		person.setName("foo");
		repositoryConsumer.getUnqualifiedRepo().save(person);
		List<Person> result = repositoryConsumer.getUnqualifiedRepo().findByName("foo");

		assertThat(result, contains(person));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void saveAndFindQualified() {

		RepositoryConsumer repositoryConsumer = container.getInstance(RepositoryConsumer.class);
		repositoryConsumer.deleteAll();

		Person person = new Person();
		person.setName("foo");
		repositoryConsumer.getUnqualifiedRepo().save(person);
		List<Person> result = repositoryConsumer.getQualifiedRepo().findByName("foo");

		assertThat(result, contains(person));
	}
	
	
	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void callMethodOnCustomRepositoryShouldSuceed() {

		RepositoryConsumer repositoryConsumer = container.getInstance(RepositoryConsumer.class);

		int result = repositoryConsumer.getUnqualifiedRepo().returnOne();
		assertThat(result, is(1));
	}

}
