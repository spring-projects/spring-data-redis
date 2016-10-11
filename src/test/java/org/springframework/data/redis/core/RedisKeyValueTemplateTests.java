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
package org.springframework.data.redis.core;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.util.ObjectUtils;

/**
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class RedisKeyValueTemplateTests {

	RedisConnectionFactory connectionFactory;
	RedisKeyValueTemplate template;
	RedisTemplate<Object, Object> nativeTemplate;

	public RedisKeyValueTemplateTests(RedisConnectionFactory connectionFactory) {

		this.connectionFactory = connectionFactory;
		ConnectionFactoryTracker.add(connectionFactory);
	}

	@Parameters
	public static List<RedisConnectionFactory> params() {

		JedisConnectionFactory jedis = new JedisConnectionFactory();
		jedis.afterPropertiesSet();

		return Collections.<RedisConnectionFactory> singletonList(jedis);
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {

		nativeTemplate = new RedisTemplate<Object, Object>();
		nativeTemplate.setConnectionFactory(connectionFactory);
		nativeTemplate.afterPropertiesSet();

		RedisMappingContext context = new RedisMappingContext();

		RedisKeyValueAdapter adapter = new RedisKeyValueAdapter(nativeTemplate, context);
		template = new RedisKeyValueTemplate(adapter, context);
	}

	@After
	public void tearDown() {

		nativeTemplate.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				connection.flushDb();
				return null;
			}
		});
	}

	@Test // DATAREDIS-425
	public void savesObjectCorrectly() {

		final Person rand = new Person();
		rand.firstname = "rand";

		template.insert(rand);

		nativeTemplate.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {

				assertThat(connection.exists(("template-test-person:" + rand.id).getBytes())).isTrue();
				return null;
			}
		});
	}

	@Test // DATAREDIS-425
	public void findProcessesCallbackReturningSingleIdCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		final Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(new RedisCallback<byte[]>() {

			@Override
			public byte[] doInRedis(RedisConnection connection) throws DataAccessException {
				return mat.id.getBytes();
			}
		}, Person.class);

		assertThat(result).hasSize(1).contains(mat);
	}

	@Test // DATAREDIS-425
	public void findProcessesCallbackReturningMultipleIdsCorrectly() {

		final Person rand = new Person();
		rand.firstname = "rand";

		final Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(new RedisCallback<List<byte[]>>() {

			@Override
			public List<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return Arrays.asList(rand.id.getBytes(), mat.id.getBytes());
			}
		}, Person.class);

		assertThat(result).contains(rand, mat);
	}

	@Test // DATAREDIS-425
	public void findProcessesCallbackReturningNullCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(new RedisCallback<List<byte[]>>() {

			@Override
			public List<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return null;
			}
		}, Person.class);

		assertThat(result).isEmpty();
	}

	@RedisHash("template-test-person")
	static class Person {

		@Id String id;
		String firstname;

		@Override
		public int hashCode() {

			int result = ObjectUtils.nullSafeHashCode(firstname);
			return result + ObjectUtils.nullSafeHashCode(id);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof Person)) {
				return false;
			}
			Person that = (Person) obj;

			if (!ObjectUtils.nullSafeEquals(this.firstname, that.firstname)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(this.id, that.id);
		}

	}
}
