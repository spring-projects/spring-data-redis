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
package org.springframework.data.redis.repository.query;

import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.junit.Assert.*;

import java.lang.reflect.Method;

import org.junit.Test;
import org.mockito.Mock;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;
import org.springframework.data.redis.core.convert.ConversionTestEntities;
import org.springframework.data.redis.repository.query.RedisOperationChain.PathAndValue;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.DefaultParameters;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * @author Christoph Strobl
 */
public class RedisQueryCreatorUnitTests {

	private @Mock RepositoryMetadata metadataMock;

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void findBySingleSimpleProperty() throws SecurityException, NoSuchMethodException {

		RedisQueryCreator creator = createQueryCreatorForMethodWithArgs(
				SampleRepository.class.getMethod("findByFirstname", String.class), new Object[] { "eddard" });

		KeyValueQuery<RedisOperationChain> query = creator.createQuery();

		assertThat(query.getCritieria().getSismember(), hasSize(1));
		assertThat(query.getCritieria().getSismember(), hasItem(new PathAndValue("firstname", "eddard")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void findByMultipleSimpleProperties() throws SecurityException, NoSuchMethodException {

		RedisQueryCreator creator = createQueryCreatorForMethodWithArgs(
				SampleRepository.class.getMethod("findByFirstnameAndAge", String.class, Integer.class), new Object[] {
						"eddard", 43 });

		KeyValueQuery<RedisOperationChain> query = creator.createQuery();

		assertThat(query.getCritieria().getSismember(), hasSize(2));
		assertThat(query.getCritieria().getSismember(), hasItem(new PathAndValue("firstname", "eddard")));
		assertThat(query.getCritieria().getSismember(), hasItem(new PathAndValue("age", 43)));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void findByMultipleSimplePropertiesUsingOr() throws SecurityException, NoSuchMethodException {

		RedisQueryCreator creator = createQueryCreatorForMethodWithArgs(
				SampleRepository.class.getMethod("findByAgeOrFirstname", Integer.class, String.class), new Object[] { 43,
						"eddard" });

		KeyValueQuery<RedisOperationChain> query = creator.createQuery();

		assertThat(query.getCritieria().getOrSismember(), hasSize(2));
		assertThat(query.getCritieria().getOrSismember(), hasItem(new PathAndValue("age", 43)));
		assertThat(query.getCritieria().getOrSismember(), hasItem(new PathAndValue("firstname", "eddard")));
	}

	private RedisQueryCreator createQueryCreatorForMethodWithArgs(Method method, Object[] args) {

		PartTree partTree = new PartTree(method.getName(), method.getReturnType());
		RedisQueryCreator creator = new RedisQueryCreator(partTree, new ParametersParameterAccessor(new DefaultParameters(
				method), args));

		return creator;
	}

	private interface SampleRepository extends Repository<ConversionTestEntities.Person, String> {

		ConversionTestEntities.Person findByFirstname(String firstname);

		ConversionTestEntities.Person findByFirstnameAndAge(String firstname, Integer age);

		ConversionTestEntities.Person findByAgeOrFirstname(Integer age, String firstname);
	}
}
