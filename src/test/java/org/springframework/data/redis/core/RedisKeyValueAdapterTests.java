/*
 * Copyright 2015-2016 the original author or authors.
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
package org.springframework.data.redis.core;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.convert.Bucket;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class RedisKeyValueAdapterTests {

	RedisKeyValueAdapter adapter;
	StringRedisTemplate template;
	RedisConnectionFactory connectionFactory;

	public RedisKeyValueAdapterTests(RedisConnectionFactory connectionFactory) throws Exception {

		if (connectionFactory instanceof InitializingBean) {
			((InitializingBean) connectionFactory).afterPropertiesSet();
		}
		this.connectionFactory = connectionFactory;
		ConnectionFactoryTracker.add(connectionFactory);
	}

	@Parameters
	public static List<RedisConnectionFactory> params() {
		return Arrays.<RedisConnectionFactory> asList(new JedisConnectionFactory(), new LettuceConnectionFactory());
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {

		template = new StringRedisTemplate(connectionFactory);
		template.afterPropertiesSet();

		RedisMappingContext mappingContext = new RedisMappingContext(
				new MappingConfiguration(new IndexConfiguration(), new KeyspaceConfiguration()));
		mappingContext.afterPropertiesSet();

		adapter = new RedisKeyValueAdapter(template, mappingContext);

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

	/**
	 * @see DATAREDIS-512
	 */
	@Test
	public void putWritesIndexDataCorrectly() {

		Person rand = new Person();
		rand.age = 24;
		rand.firstname = "rand";

		adapter.put("rand", rand, "persons");

		assertThat(template.hasKey("persons:firstname:rand"), is(true));
		assertThat(template.hasKey("persons:rand:idx"), is(true));
		assertThat(template.opsForSet().isMember("persons:rand:idx", "persons:firstname:rand"), is(true));

		Person mat = new Person();
		mat.age = 22;
		mat.firstname = "mat";
		adapter.put("mat", mat, "persons");

		assertThat(template.hasKey("persons:firstname:rand"), is(true));
		assertThat(template.hasKey("persons:firstname:mat"), is(true));
		assertThat(template.hasKey("persons:rand:idx"), is(true));
		assertThat(template.hasKey("persons:mat:idx"), is(true));
		assertThat(template.opsForSet().isMember("persons:rand:idx", "persons:firstname:rand"), is(true));
		assertThat(template.opsForSet().isMember("persons:mat:idx", "persons:firstname:mat"), is(true));

		rand.firstname = "frodo";
		adapter.put("rand", rand, "persons");

		assertThat(template.hasKey("persons:firstname:rand"), is(false));
		assertThat(template.hasKey("persons:firstname:mat"), is(true));
		assertThat(template.hasKey("persons:firstname:frodo"), is(true));
		assertThat(template.hasKey("persons:rand:idx"), is(true));
		assertThat(template.opsForSet().isMember("persons:rand:idx", "persons:firstname:frodo"), is(true));
		assertThat(template.opsForSet().isMember("persons:mat:idx", "persons:firstname:mat"), is(true));
	}

	/**
	 * @see DATAREDIS-471
	 */
	@Test
	public void updateShouldAlterIndexDataCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:firstname:rand"), is(true));

		PartialUpdate<Person> update = new PartialUpdate<Person>("1", Person.class) //
				.set("firstname", "mat");

		adapter.update(update);

		assertThat(template.hasKey("persons:firstname:rand"), is(false));
		assertThat(template.hasKey("persons:firstname:mat"), is(true));
	}

	/**
	 * @see DATAREDIS-471
	 */
	@Test
	public void updateShouldAlterIndexDataOnNestedObjectCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:address.country:andor"), is(true));

		PartialUpdate<Person> update = new PartialUpdate<Person>("1", Person.class);
		Address addressUpdate = new Address();
		addressUpdate.country = "tear";

		update = update.set("address", addressUpdate);

		adapter.update(update);

		assertThat(template.hasKey("persons:address.country:andor"), is(false));
		assertThat(template.hasKey("persons:address.country:tear"), is(true));
	}

	/**
	 * @see DATAREDIS-471
	 */
	@Test
	public void updateShouldAlterIndexDataOnNestedObjectPathCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:address.country:andor"), is(true));

		PartialUpdate<Person> update = new PartialUpdate<Person>("1", Person.class) //
				.set("address.country", "tear");

		adapter.update(update);

		assertThat(template.hasKey("persons:address.country:andor"), is(false));
		assertThat(template.hasKey("persons:address.country:tear"), is(true));
	}

	/**
	 * @see DATAREDIS-471
	 */
	@Test
	public void updateShouldRemoveComplexObjectCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";
		rand.address.city = "emond's field";

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<Person>("1", Person.class) //
				.del("address");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "address.country"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "address.city"), is(false));
		assertThat(template.opsForSet().isMember("persons:address.country:andor", "1"), is(false));
	}

	/**
	 * @see DATAREDIS-471
	 */
	@Test
	public void updateShouldRemoveSimpleListValuesCorrectly() {

		Person rand = new Person();
		rand.nicknames = Arrays.asList("lews therin", "dragon reborn");

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<Person>("1", Person.class) //
				.del("nicknames");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "nicknames.[0]"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "nicknames.[1]"), is(false));
	}

	/**
	 * @see DATAREDIS-471
	 */
	@Test
	public void updateShouldRemoveComplexListValuesCorrectly() {

		Person mat = new Person();
		mat.firstname = "mat";
		mat.nicknames = Collections.singletonList("prince of ravens");

		Person perrin = new Person();
		perrin.firstname = "mat";
		perrin.nicknames = Collections.singletonList("lord of the two rivers");

		Person rand = new Person();
		rand.coworkers = Arrays.asList(mat, perrin);

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<Person>("1", Person.class) //
				.del("coworkers");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[0].firstname"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[0].nicknames.[0]"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[1].firstname"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[1].nicknames.[0]"), is(false));
	}

	/**
	 * @see DATAREDIS-471
	 */
	@Test
	public void updateShouldRemoveSimpleMapValuesCorrectly() {

		Person rand = new Person();
		rand.physicalAttributes = Collections.singletonMap("eye-color", "grey");

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<Person>("1", Person.class) //
				.del("physicalAttributes");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "physicalAttributes.[eye-color]"), is(false));
	}

	/**
	 * @see DATAREDIS-471
	 */
	@Test
	public void updateShouldRemoveComplexMapValuesCorrectly() {

		Person tam = new Person();
		tam.firstname = "tam";

		Person rand = new Person();
		rand.relatives = Collections.singletonMap("stepfather", tam);

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<Person>("1", Person.class) //
				.del("relatives");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "relatives.[stepfather].firstname"), is(false));
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
