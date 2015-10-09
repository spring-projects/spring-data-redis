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

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.hamcrest.core.IsNot.*;
import static org.junit.Assert.*;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.convert.Bucket;
import org.springframework.data.redis.core.convert.IndexResolverImpl;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.convert.MappingRedisConverter;
import org.springframework.data.redis.core.convert.ReferenceResolverImpl;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;

/**
 * @author Christoph Strobl
 */
public class RedisKeyValueAdapterTests {

	RedisKeyValueAdapter adapter;
	StringRedisTemplate template;
	RedisConnectionFactory connectionFactory;

	@Before
	public void setUp() {

		JedisConnectionFactory jedisConnectionFactory = new JedisConnectionFactory();
		jedisConnectionFactory.afterPropertiesSet();
		connectionFactory = jedisConnectionFactory;

		template = new StringRedisTemplate(connectionFactory);
		template.afterPropertiesSet();

		RedisMappingContext mappingContext = new RedisMappingContext(new MappingConfiguration(new IndexConfiguration(),
				new KeyspaceConfiguration()));
		mappingContext.afterPropertiesSet();

		MappingRedisConverter converter = new MappingRedisConverter(mappingContext, new IndexResolverImpl(), null);

		adapter = new RedisKeyValueAdapter(template, converter);
		converter.setReferenceResolver(new ReferenceResolverImpl(adapter));
		converter.afterPropertiesSet();

		template.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {
				connection.flushDb();
				return null;
			}
		});
	}

	@After
	public void tearDown() {

		try {
			adapter.destroy();
		} catch (Exception e) {
			// ignore
		}
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void putWritesDataCorrectly() {

		Person rand = new Person();
		rand.age = 24;

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*"), hasItems("persons", "persons:1"));
		assertThat(template.opsForSet().size("persons"), is(1L));
		assertThat(template.opsForSet().members("persons"), hasItems("1"));
		assertThat(template.opsForHash().entries("persons:1").size(), is(2));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void putWritesSimpleIndexDataCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*"), hasItem("persons:firstname:rand"));
		assertThat(template.opsForSet().members("persons:firstname:rand"), hasItems("1"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void putWritesNestedDataCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.city = "Emond's Field";

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*"), hasItems("persons", "persons:1"));
		assertThat(template.opsForHash().entries("persons:1").size(), is(2));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void putWritesSimpleNestedIndexValuesCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "Andor";

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*"), hasItem("persons:address.country:Andor"));
		assertThat(template.opsForSet().members("persons:address.country:Andor"), hasItems("1"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getShouldReadSimpleObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_class", Person.class.getName());
		map.put("age", "24");
		template.opsForHash().putAll("persons:load-1", map);

		Object loaded = adapter.get("load-1", "persons");

		assertThat(loaded, instanceOf(Person.class));
		assertThat(((Person) loaded).age, is(24));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void getShouldReadNestedObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_class", Person.class.getName());
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:load-1", map);

		Object loaded = adapter.get("load-1", "persons");

		assertThat(loaded, instanceOf(Person.class));
		assertThat(((Person) loaded).address.country, is("Andor"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void couldReadsKeyspaceSizeCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_class", Person.class.getName());
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:load-1", map);

		template.opsForSet().add("persons", "1", "2", "3");

		assertThat(adapter.count("persons"), is(3L));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void deleteRemovesEntriesCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_class", Person.class.getName());
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:1", map);
		template.opsForSet().add("persons", "1");

		adapter.delete("1", "persons");

		assertThat(template.opsForSet().members("persons"), not(hasItem("1")));
		assertThat(template.hasKey("persons:1"), is(false));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void deleteCleansIndexedDataCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_class", Person.class.getName());
		map.put("firstname", "rand");
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:1", map);
		template.opsForSet().add("persons", "1");
		template.opsForSet().add("persons:1:idx", "persons:firstname:rand");
		template.opsForSet().add("persons:firstname:rand", "1");

		adapter.delete("1", "persons");

		assertThat(template.opsForSet().members("persons:firstname:rand"), not(hasItem("1")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void keyExpiredEventShouldRemoveHelperStructures() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_class", Person.class.getName());
		map.put("firstname", "rand");
		map.put("address.country", "Andor");

		template.opsForSet().add("persons", "1");
		template.opsForSet().add("persons:firstname:rand", "1");
		template.opsForSet().add("persons:1:idx", "persons:firstname:rand");

		adapter.onApplicationEvent(new RedisKeyExpiredEvent("persons:1".getBytes(Bucket.CHARSET)));

		assertThat(template.hasKey("persons:firstname:rand"), is(false));
		assertThat(template.hasKey("persons:1:idx"), is(false));
		assertThat(template.opsForSet().members("persons"), not(hasItem("1")));
	}

	@KeySpace("persons")
	static class Person {

		@Id String id;
		@Indexed String firstname;
		Gender gender;

		List<String> nicknames;
		List<Person> coworkers;
		Integer age;
		Boolean alive;
		Date birthdate;

		Address address;

		Map<String, String> physicalAttributes;
		Map<String, Person> relatives;

		@Reference Location location;
		@Reference List<Location> visited;
	}

	static class Address {

		String city;
		@Indexed String country;
	}

	static class AddressWithId extends Address {

		@Id String id;
	}

	static enum Gender {
		MALE, FEMALE
	}

	static class AddressWithPostcode extends Address {

		String postcode;
	}

	static class TaVeren extends Person {

	}

	@KeySpace("locations")
	static class Location {

		@Id String id;
		String name;
	}

}
