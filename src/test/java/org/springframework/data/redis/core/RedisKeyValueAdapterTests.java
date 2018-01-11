/*
 * Copyright 2015-2018 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.geo.Point;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.core.RedisKeyValueAdapter.EnableKeyspaceEvents;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.index.GeoIndexed;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class RedisKeyValueAdapterTests {

	private static Set<RedisConnectionFactory> initializedFactories = new HashSet<>();

	RedisKeyValueAdapter adapter;
	StringRedisTemplate template;
	RedisConnectionFactory connectionFactory;

	public RedisKeyValueAdapterTests(RedisConnectionFactory connectionFactory) throws Exception {

		if (connectionFactory instanceof InitializingBean && initializedFactories.add(connectionFactory)) {
			((InitializingBean) connectionFactory).afterPropertiesSet();
		}

		this.connectionFactory = connectionFactory;
		ConnectionFactoryTracker.add(connectionFactory);
	}

	@Parameters
	public static List<RedisConnectionFactory> params() {

		LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory();
		lettuceConnectionFactory.setClientResources(LettuceTestClientResources.getSharedClientResources());
		return Arrays.<RedisConnectionFactory> asList(new JedisConnectionFactory(), lettuceConnectionFactory);
	}

	@AfterClass
	public static void cleanUp() {
		initializedFactories.clear();
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
		adapter.setEnableKeyspaceEvents(EnableKeyspaceEvents.ON_STARTUP);
		adapter.afterPropertiesSet();

		template.execute((RedisCallback<Void>) connection -> {
			connection.flushDb();
			return null;
		});

		RedisConnection connection = template.getConnectionFactory().getConnection();

		try {
			connection.setConfig("notify-keyspace-events", "KEA");
		} finally {
			connection.close();
		}
	}

	@After
	public void tearDown() {

		try {
			adapter.destroy();
		} catch (Exception e) {
			// ignore
		}
	}

	@Test // DATAREDIS-425
	public void putWritesDataCorrectly() {

		Person rand = new Person();
		rand.age = 24;

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*"), hasItems("persons", "persons:1"));
		assertThat(template.opsForSet().size("persons"), is(1L));
		assertThat(template.opsForSet().members("persons"), hasItems("1"));
		assertThat(template.opsForHash().entries("persons:1").size(), is(2));
	}

	@Test // DATAREDIS-744
	public void putWritesDataWithColonCorrectly() {

		Person rand = new Person();
		rand.age = 24;

		adapter.put("1:a", rand, "persons");

		assertThat(template.keys("persons*"), hasItems("persons", "persons:1:a"));
		assertThat(template.opsForSet().size("persons"), is(1L));
		assertThat(template.opsForSet().members("persons"), hasItems("1:a"));
		assertThat(template.opsForHash().entries("persons:1:a").size(), is(2));
	}

	@Test // DATAREDIS-425
	public void putWritesSimpleIndexDataCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*"), hasItem("persons:firstname:rand"));
		assertThat(template.opsForSet().members("persons:firstname:rand"), hasItems("1"));
	}

	@Test // DATAREDIS-744
	public void putWritesSimpleIndexDataWithColonCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		adapter.put("1:a", rand, "persons");

		assertThat(template.keys("persons*"), hasItem("persons:firstname:rand"));
		assertThat(template.opsForSet().members("persons:firstname:rand"), hasItems("1:a"));
	}

	@Test // DATAREDIS-425
	public void putWritesNestedDataCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.city = "Emond's Field";

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*"), hasItems("persons", "persons:1"));
		assertThat(template.opsForHash().entries("persons:1").size(), is(2));
	}

	@Test // DATAREDIS-425
	public void putWritesSimpleNestedIndexValuesCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "Andor";

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*"), hasItem("persons:address.country:Andor"));
		assertThat(template.opsForSet().members("persons:address.country:Andor"), hasItems("1"));
	}

	@Test // DATAREDIS-425
	public void getShouldReadSimpleObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("age", "24");
		template.opsForHash().putAll("persons:load-1", map);

		Object loaded = adapter.get("load-1", "persons");

		assertThat(loaded, instanceOf(Person.class));
		assertThat(((Person) loaded).age, is(24));
	}

	@Test // DATAREDIS-744
	public void getShouldReadSimpleObjectWithColonInIdCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_class", Person.class.getName());
		map.put("age", "24");
		template.opsForHash().putAll("persons:load-1:a", map);

		Object loaded = adapter.get("load-1:a", "persons");

		assertThat(loaded, instanceOf(Person.class));
		assertThat(((Person) loaded).age, is(24));
	}

	@Test // DATAREDIS-425
	public void getShouldReadNestedObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:load-1", map);

		Object loaded = adapter.get("load-1", "persons");

		assertThat(loaded, instanceOf(Person.class));
		assertThat(((Person) loaded).address.country, is("Andor"));
	}

	@Test // DATAREDIS-425
	public void couldReadsKeyspaceSizeCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:load-1", map);

		template.opsForSet().add("persons", "1", "2", "3");

		assertThat(adapter.count("persons"), is(3L));
	}

	@Test // DATAREDIS-425
	public void deleteRemovesEntriesCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:1", map);
		template.opsForSet().add("persons", "1");

		adapter.delete("1", "persons");

		assertThat(template.opsForSet().members("persons"), not(hasItem("1")));
		assertThat(template.hasKey("persons:1"), is(false));
	}

	@Test // DATAREDIS-425
	public void deleteCleansIndexedDataCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
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

	@Test // DATAREDIS-425
	public void keyExpiredEventShouldRemoveHelperStructures() throws Exception {

		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("firstname", "rand");
		map.put("address.country", "Andor");

		template.opsForHash().putAll("persons:1", map);

		template.opsForSet().add("persons", "1");
		template.opsForSet().add("persons:firstname:rand", "1");
		template.opsForSet().add("persons:1:idx", "persons:firstname:rand");

		template.expire("persons:1", 100, TimeUnit.MILLISECONDS);

		waitUntilKeyIsGone(template, "persons:1");
		waitUntilKeyIsGone(template, "persons:1:phantom");
		waitUntilKeyIsGone(template, "persons:firstname:rand");

		assertThat(template.hasKey("persons:1"), is(false));
		assertThat(template.hasKey("persons:firstname:rand"), is(false));
		assertThat(template.hasKey("persons:1:idx"), is(false));
		assertThat(template.opsForSet().members("persons"), not(hasItem("1")));
	}

	@Test // DATAREDIS-744
	public void keyExpiredEventShouldRemoveHelperStructuresForObjectsWithColonInId() throws Exception {

		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_class", Person.class.getName());
		map.put("firstname", "rand");
		map.put("address.country", "Andor");

		template.opsForHash().putAll("persons:1:b", map);

		template.opsForSet().add("persons", "1");
		template.opsForSet().add("persons:firstname:rand", "1:b");
		template.opsForSet().add("persons:1:b:idx", "persons:firstname:rand");

		template.expire("persons:1:b", 100, TimeUnit.MILLISECONDS);

		waitUntilKeyIsGone(template, "persons:1:b");
		waitUntilKeyIsGone(template, "persons:1:b:phantom");
		waitUntilKeyIsGone(template, "persons:firstname:rand");

		assertThat(template.hasKey("persons:1"), is(false));
		assertThat(template.hasKey("persons:firstname:rand"), is(false));
		assertThat(template.hasKey("persons:1:b:idx"), is(false));
		assertThat(template.opsForSet().members("persons"), not(hasItem("1:b")));
	}

	@Test // DATAREDIS-589
	public void keyExpiredEventWithoutKeyspaceShouldBeIgnored() throws Exception {

		assumeTrue(RedisTestProfileValueSource.matches("runLongTests", "true"));

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("firstname", "rand");
		map.put("address.country", "Andor");

		template.opsForHash().putAll("persons:1", map);
		template.opsForHash().putAll("1", map);

		template.opsForSet().add("persons", "1");
		template.opsForSet().add("persons:firstname:rand", "1");
		template.opsForSet().add("persons:1:idx", "persons:firstname:rand");

		template.expire("1", 100, TimeUnit.MILLISECONDS);

		waitUntilKeyIsGone(template, "1");

		assertThat(template.hasKey("persons:1"), is(true));
		assertThat(template.hasKey("persons:firstname:rand"), is(true));
		assertThat(template.hasKey("persons:1:idx"), is(true));
		assertThat(template.opsForSet().members("persons"), hasItem("1"));
	}

	@Test // DATAREDIS-512
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

	@Test // DATAREDIS-471
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

	@Test // DATAREDIS-744
	public void updateShouldAlterIndexDataForObjectsWithColonInIdCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		adapter.put("1:a", rand, "persons");

		assertThat(template.hasKey("persons:firstname:rand"), is(true));

		PartialUpdate<Person> update = new PartialUpdate<>("1:a", Person.class) //
				.set("firstname", "mat");

		adapter.update(update);

		assertThat(template.hasKey("persons:firstname:rand"), is(false));
		assertThat(template.hasKey("persons:firstname:mat"), is(true));
	}

	@Test // DATAREDIS-471
	public void updateShouldAlterIndexDataOnNestedObjectCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:address.country:andor"), is(true));

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class);
		Address addressUpdate = new Address();
		addressUpdate.country = "tear";

		update = update.set("address", addressUpdate);

		adapter.update(update);

		assertThat(template.hasKey("persons:address.country:andor"), is(false));
		assertThat(template.hasKey("persons:address.country:tear"), is(true));
	}

	@Test // DATAREDIS-471
	public void updateShouldAlterIndexDataOnNestedObjectPathCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:address.country:andor"), is(true));

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.set("address.country", "tear");

		adapter.update(update);

		assertThat(template.hasKey("persons:address.country:andor"), is(false));
		assertThat(template.hasKey("persons:address.country:tear"), is(true));
	}

	@Test // DATAREDIS-471
	public void updateShouldRemoveComplexObjectCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";
		rand.address.city = "emond's field";

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("address");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "address.country"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "address.city"), is(false));
		assertThat(template.opsForSet().isMember("persons:address.country:andor", "1"), is(false));
	}

	@Test // DATAREDIS-471
	public void updateShouldRemoveSimpleListValuesCorrectly() {

		Person rand = new Person();
		rand.nicknames = Arrays.asList("lews therin", "dragon reborn");

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("nicknames");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "nicknames.[0]"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "nicknames.[1]"), is(false));
	}

	@Test // DATAREDIS-471
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

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("coworkers");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[0].firstname"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[0].nicknames.[0]"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[1].firstname"), is(false));
		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[1].nicknames.[0]"), is(false));
	}

	@Test // DATAREDIS-471
	public void updateShouldRemoveSimpleMapValuesCorrectly() {

		Person rand = new Person();
		rand.physicalAttributes = Collections.singletonMap("eye-color", "grey");

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("physicalAttributes");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "physicalAttributes.[eye-color]"), is(false));
	}

	@Test // DATAREDIS-471
	public void updateShouldRemoveComplexMapValuesCorrectly() {

		Person tam = new Person();
		tam.firstname = "tam";

		Person rand = new Person();
		rand.relatives = Collections.singletonMap("stepfather", tam);

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("relatives");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "relatives.[stepfather].firstname"), is(false));
	}

	@Test // DATAREDIS-533
	public void putShouldCreateGeoIndexCorrectly() {

		Person tam = new Person();
		tam.id = "1";
		tam.firstname = "tam";
		tam.address = new Address();
		tam.address.location = new Point(10, 20);

		adapter.put("1", tam, "persons");

		assertThat(template.opsForZSet().score("persons:address:location", "1"), is(notNullValue()));
	}

	@Test // DATAREDIS-533
	public void deleteShouldRemoveGeoIndexCorrectly() {

		Person tam = new Person();
		tam.id = "1";
		tam.firstname = "tam";
		tam.address = new Address();
		tam.address.location = new Point(10, 20);

		adapter.put("1", tam, "persons");

		adapter.delete("1", "persons", Person.class);

		assertThat(template.opsForZSet().score("persons:address:location", "1"), is(nullValue()));
	}

	@Test // DATAREDIS-533
	public void updateShouldAlterGeoIndexCorrectlyOnDelete() {

		Person tam = new Person();
		tam.id = "1";
		tam.firstname = "tam";
		tam.address = new Address();
		tam.address.location = new Point(10, 20);

		adapter.put("1", tam, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("address.location");

		adapter.update(update);

		assertThat(template.opsForZSet().score("persons:address:location", "1"), is(nullValue()));
	}

	@Test // DATAREDIS-533, DATAREDIS-614
	public void updateShouldAlterGeoIndexCorrectlyOnUpdate() {

		Person tam = new Person();
		tam.id = "1";
		tam.firstname = "tam";
		tam.address = new Address();
		tam.address.location = new Point(10, 20);

		adapter.put("1", tam, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.set("address.location", new Point(17, 18));

		adapter.update(update);

		assertThat(template.opsForZSet().score("persons:address:location", "1"), is(notNullValue()));
		Point updatedLocation = template.opsForGeo().position("persons:address:location", "1").iterator().next();

		assertThat(updatedLocation.getX(), is(closeTo(17D, 0.005)));
		assertThat(updatedLocation.getY(), is(closeTo(18D, 0.005)));
	}

	/**
	 * Wait up to 5 seconds until {@code key} is no longer available in Redis.
	 *
	 * @param template must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @throws TimeoutException
	 * @throws InterruptedException
	 */
	private static void waitUntilKeyIsGone(RedisTemplate<String, ?> template, String key)
			throws TimeoutException, InterruptedException {
		waitUntilKeyIsGone(template, key, 5, TimeUnit.SECONDS);
	}

	/**
	 * Wait up to {@code timeout} until {@code key} is no longer available in Redis.
	 *
	 * @param template must not be {@literal null}.
	 * @param key must not be {@literal null}.
	 * @param timeout
	 * @param timeUnit must not be {@literal null}.
	 * @throws InterruptedException
	 * @throws TimeoutException
	 */
	private static void waitUntilKeyIsGone(RedisTemplate<String, ?> template, String key, long timeout, TimeUnit timeUnit)
			throws InterruptedException, TimeoutException {

		long limitMs = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
		long sleepMs = 100;
		long waitedMs = 0;

		while (template.hasKey(key)) {

			if (waitedMs > limitMs) {
				throw new TimeoutException(String.format("Key '%s' after %d %s still present", key, timeout, timeUnit));
			}

			Thread.sleep(sleepMs);
			waitedMs += sleepMs;
		}
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
		@GeoIndexed Point location;

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
