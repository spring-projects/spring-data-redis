/*
 * Copyright 2016-2024 the original author or authors.
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
package org.springframework.data.redis.mapping;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.redis.Address;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.hash.Jackson2HashMapper;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;

/**
 * Integration tests for {@link Jackson2HashMapper}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author John Blum
 */
@MethodSource("params")
public class Jackson2HashMapperIntegrationTests {

	RedisTemplate<String, Object> template;
	RedisConnectionFactory factory;
	Jackson2HashMapper mapper;

	public Jackson2HashMapperIntegrationTests(RedisConnectionFactory factory) throws Exception {

		this.factory = factory;
		if (factory instanceof InitializingBean initializingBean) {
			initializingBean.afterPropertiesSet();
		}
	}

	public static Collection<RedisConnectionFactory> params() {

		return Arrays.asList(JedisConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class),
				LettuceConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class));
	}

	@BeforeEach
	public void setUp() {

		this.template = new RedisTemplate<>();
		this.template.setConnectionFactory(factory);
		template.afterPropertiesSet();

		this.mapper = new Jackson2HashMapper(true);
	}

	@ParameterizedRedisTest // DATAREDIS-423
	public void shouldWriteReadHashCorrectly() {

		Person jon = new Person("jon", "snow", 19);
		Address adr = new Address();
		adr.setStreet("the wall");
		adr.setNumber(100);
		jon.setAddress(adr);

		template.opsForHash().putAll("JON-SNOW", mapper.toHash(jon));

		Person result = (Person) mapper.fromHash(template.<String, Object> opsForHash().entries("JON-SNOW"));
		assertThat(result).isEqualTo(jon);
	}

	@ParameterizedRedisTest // GH-2565
	public void shouldPreserveListPropertyOrderOnHashedSource() {

		User jonDoe = User.as("Jon Doe")
			.withPhoneNumber(9, 7, 1, 5, 5, 5, 4, 1, 8, 2);

		template.opsForHash().putAll("JON-DOE", mapper.toHash(jonDoe));

		User deserializedJonDoe =
			(User) mapper.fromHash(template.<String, Object>opsForHash().entries("JON-DOE"));

		assertThat(deserializedJonDoe).isNotNull();
		assertThat(deserializedJonDoe).isNotSameAs(jonDoe);
		assertThat(deserializedJonDoe.getName()).isEqualTo("Jon Doe");
		assertThat(deserializedJonDoe.getPhoneNumber()).containsExactly(9, 7, 1, 5, 5, 5, 4, 1, 8, 2);
	}

	static class User {

		static User as(String name) {
			return new User(name);
		}

		private String name;
		private List<Integer> phoneNumber;

		User() { }

		User(String name) {
			this.name = name;
		}

		User withPhoneNumber(Integer... numbers) {
			this.phoneNumber = new ArrayList<>(Arrays.asList(numbers));
			return this;
		}

		public String getName() {
			return this.name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public List<Integer> getPhoneNumber() {
			return this.phoneNumber;
		}

		public void setPhoneNumber(List<Integer> phoneNumber) {
			this.phoneNumber = phoneNumber;
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof User that)) {
				return false;
			}

			return Objects.equals(this.getName(), that.getName())
				&& Objects.equals(this.getPhoneNumber(), that.getPhoneNumber());
		}

		@Override
		public int hashCode() {
			return Objects.hash(getName(), getPhoneNumber());
		}

		@Override
		public String toString() {
			return getName();
		}
	}
}
