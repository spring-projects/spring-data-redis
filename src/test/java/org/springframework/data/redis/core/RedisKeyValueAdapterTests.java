/*
 * Copyright 2015-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Offset.offset;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Reference;
import org.springframework.data.annotation.TypeAlias;
import org.springframework.data.geo.Point;
import org.springframework.data.keyvalue.annotation.KeySpace;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.RedisKeyValueAdapter.EnableKeyspaceEvents;
import org.springframework.data.redis.core.RedisKeyValueAdapter.ShadowCopy;
import org.springframework.data.redis.core.convert.KeyspaceConfiguration;
import org.springframework.data.redis.core.convert.MappingConfiguration;
import org.springframework.data.redis.core.index.GeoIndexed;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.test.condition.EnabledIfLongRunningTest;

/**
 * Integration tests for {@link RedisKeyValueAdapter}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Andrey Muchnik
 */
@ExtendWith(LettuceConnectionFactoryExtension.class)
public class RedisKeyValueAdapterTests {

	private RedisKeyValueAdapter adapter;
	private StringRedisTemplate template;
	private RedisConnectionFactory connectionFactory;
	private RedisMappingContext mappingContext;

	public RedisKeyValueAdapterTests(RedisConnectionFactory connectionFactory) throws Exception {
		this.connectionFactory = connectionFactory;
	}

	@BeforeEach
	void setUp() {

		template = new StringRedisTemplate(connectionFactory);
		template.afterPropertiesSet();

		mappingContext = new RedisMappingContext(
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

	@AfterEach
	void tearDown() {

		try {
			adapter.destroy();
		} catch (Exception ignore) {
		}
	}

	@Test // DATAREDIS-425
	void putWritesDataCorrectly() {

		Person rand = new Person();
		rand.age = 24;

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*")).contains("persons", "persons:1");
		assertThat(template.opsForSet().size("persons")).isEqualTo(1L);
		assertThat(template.opsForSet().members("persons")).contains("1");
		assertThat(template.opsForHash().entries("persons:1").size()).isEqualTo(2);
	}

	@Test // DATAREDIS-744
	void putWritesDataWithColonCorrectly() {

		Person rand = new Person();
		rand.age = 24;

		adapter.put("1:a", rand, "persons");

		assertThat(template.keys("persons*")).contains("persons", "persons:1:a");
		assertThat(template.opsForSet().size("persons")).isEqualTo(1L);
		assertThat(template.opsForSet().members("persons")).contains("1:a");
		assertThat(template.opsForHash().entries("persons:1:a").size()).isEqualTo(2);
	}

	@Test // DATAREDIS-425
	void putWritesSimpleIndexDataCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*")).contains("persons:firstname:rand");
		assertThat(template.opsForSet().members("persons:firstname:rand")).contains("1");
	}

	@Test // DATAREDIS-744
	void putWritesSimpleIndexDataWithColonCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		adapter.put("1:a", rand, "persons");

		assertThat(template.keys("persons*")).contains("persons:firstname:rand");
		assertThat(template.opsForSet().members("persons:firstname:rand")).contains("1:a");
	}

	@Test // DATAREDIS-425
	void putWritesNestedDataCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.city = "Emond's Field";

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*")).contains("persons", "persons:1");
		assertThat(template.opsForHash().entries("persons:1").size()).isEqualTo(2);
	}

	@Test // DATAREDIS-425
	void putWritesSimpleNestedIndexValuesCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "Andor";

		adapter.put("1", rand, "persons");

		assertThat(template.keys("persons*")).contains("persons:address.country:Andor");
		assertThat(template.opsForSet().members("persons:address.country:Andor")).contains("1");
	}

	@Test // DATAREDIS-425
	void getShouldReadSimpleObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("age", "24");
		template.opsForHash().putAll("persons:load-1", map);

		Object loaded = adapter.get("load-1", "persons");

		assertThat(loaded).isInstanceOf(Person.class);
		assertThat(((Person) loaded).age).isEqualTo(24);
	}

	@Test // DATAREDIS-744
	void getShouldReadSimpleObjectWithColonInIdCorrectly() {

		Map<String, String> map = new LinkedHashMap<String, String>();
		map.put("_class", Person.class.getName());
		map.put("age", "24");
		template.opsForHash().putAll("persons:load-1:a", map);

		Object loaded = adapter.get("load-1:a", "persons");

		assertThat(loaded).isInstanceOf(Person.class);
		assertThat(((Person) loaded).age).isEqualTo(24);
	}

	@Test // DATAREDIS-425
	void getShouldReadNestedObjectCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:load-1", map);

		Object loaded = adapter.get("load-1", "persons");

		assertThat(loaded).isInstanceOf(Person.class);
		assertThat(((Person) loaded).address.country).isEqualTo("Andor");
	}

	@Test // GH-1995
	void getAllOfShouldReturnSuperTypeForUnregisteredTypeAlias() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", "taveren");
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:load-1", map);

		Object loaded = adapter.get("load-1", "persons", Person.class);

		assertThat(loaded).isExactlyInstanceOf(Person.class);
	}

	@Test // GH-1995
	void getAllOfShouldReturnCorrectTypeForRegisteredTypeAlias() {

		mappingContext.getPersistentEntity(TaVeren.class);

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", "taveren");
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:load-1", map);

		Object loaded = adapter.get("load-1", "persons", Person.class);

		assertThat(loaded).isExactlyInstanceOf(TaVeren.class);
	}

	@Test // DATAREDIS-425
	void couldReadsKeyspaceSizeCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:load-1", map);

		template.opsForSet().add("persons", "1", "2", "3");

		assertThat(adapter.count("persons")).isEqualTo(3L);
	}

	@Test // DATAREDIS-425
	void deleteRemovesEntriesCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:1", map);
		template.opsForSet().add("persons", "1");

		adapter.delete("1", "persons");

		assertThat(template.opsForSet().members("persons")).doesNotContain("1");
		assertThat(template.hasKey("persons:1")).isFalse();
	}

	@Test // DATAREDIS-425
	void deleteCleansIndexedDataCorrectly() {

		Map<String, String> map = new LinkedHashMap<>();
		map.put("_class", Person.class.getName());
		map.put("firstname", "rand");
		map.put("address.country", "Andor");
		template.opsForHash().putAll("persons:1", map);
		template.opsForSet().add("persons", "1");
		template.opsForSet().add("persons:1:idx", "persons:firstname:rand");
		template.opsForSet().add("persons:firstname:rand", "1");

		adapter.delete("1", "persons");

		assertThat(template.opsForSet().members("persons:firstname:rand")).doesNotContain("1");
	}

	@Test // DATAREDIS-1106
	void deleteRemovesExpireHelperStructures() {

		WithExpiration source = new WithExpiration();
		source.id = "1";
		source.value = "vale";

		adapter.put("1", source, "withexpiration");

		assertThat(template.hasKey("withexpiration:1")).isTrue();
		assertThat(template.hasKey("withexpiration:1:phantom")).isTrue();

		adapter.delete("1", "withexpiration", WithExpiration.class);

		assertThat(template.hasKey("withexpiration:1")).isFalse();
		assertThat(template.hasKey("withexpiration:1:phantom")).isFalse();
	}

	@Test // DATAREDIS-425
	@EnabledIfLongRunningTest
	void keyExpiredEventShouldRemoveHelperStructures() throws Exception {

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

		assertThat(template.hasKey("persons:1")).isFalse();
		assertThat(template.hasKey("persons:firstname:rand")).isFalse();
		assertThat(template.hasKey("persons:1:idx")).isFalse();
		assertThat(template.opsForSet().members("persons")).doesNotContain("1");
	}

	@Test // DATAREDIS-744
	@EnabledIfLongRunningTest
	void keyExpiredEventShouldRemoveHelperStructuresForObjectsWithColonInId() throws Exception {

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

		assertThat(template.hasKey("persons:1")).isFalse();
		assertThat(template.hasKey("persons:firstname:rand")).isFalse();
		assertThat(template.hasKey("persons:1:b:idx")).isFalse();
		assertThat(template.opsForSet().members("persons")).doesNotContain("1:b");
	}

	@Test // DATAREDIS-589
	@EnabledIfLongRunningTest
	void keyExpiredEventWithoutKeyspaceShouldBeIgnored() throws Exception {

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

		assertThat(template.hasKey("persons:1")).isTrue();
		assertThat(template.hasKey("persons:firstname:rand")).isTrue();
		assertThat(template.hasKey("persons:1:idx")).isTrue();
		assertThat(template.opsForSet().members("persons")).contains("1");
	}

	@Test // DATAREDIS-512
	void putWritesIndexDataCorrectly() {

		Person rand = new Person();
		rand.age = 24;
		rand.firstname = "rand";

		adapter.put("rand", rand, "persons");

		assertThat(template.hasKey("persons:firstname:rand")).isTrue();
		assertThat(template.hasKey("persons:rand:idx")).isTrue();
		assertThat(template.opsForSet().isMember("persons:rand:idx", "persons:firstname:rand")).isTrue();

		Person mat = new Person();
		mat.age = 22;
		mat.firstname = "mat";
		adapter.put("mat", mat, "persons");

		assertThat(template.hasKey("persons:firstname:rand")).isTrue();
		assertThat(template.hasKey("persons:firstname:mat")).isTrue();
		assertThat(template.hasKey("persons:rand:idx")).isTrue();
		assertThat(template.hasKey("persons:mat:idx")).isTrue();
		assertThat(template.opsForSet().isMember("persons:rand:idx", "persons:firstname:rand")).isTrue();
		assertThat(template.opsForSet().isMember("persons:mat:idx", "persons:firstname:mat")).isTrue();

		rand.firstname = "frodo";
		adapter.put("rand", rand, "persons");

		assertThat(template.hasKey("persons:firstname:rand")).isFalse();
		assertThat(template.hasKey("persons:firstname:mat")).isTrue();
		assertThat(template.hasKey("persons:firstname:frodo")).isTrue();
		assertThat(template.hasKey("persons:rand:idx")).isTrue();
		assertThat(template.opsForSet().isMember("persons:rand:idx", "persons:firstname:frodo")).isTrue();
		assertThat(template.opsForSet().isMember("persons:mat:idx", "persons:firstname:mat")).isTrue();
	}

	@Test // DATAREDIS-471
	void updateShouldAlterIndexDataCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:firstname:rand")).isTrue();

		PartialUpdate<Person> update = new PartialUpdate<Person>("1", Person.class) //
				.set("firstname", "mat");

		adapter.update(update);

		assertThat(template.hasKey("persons:firstname:rand")).isFalse();
		assertThat(template.hasKey("persons:firstname:mat")).isTrue();
	}

	@Test // DATAREDIS-744
	void updateShouldAlterIndexDataForObjectsWithColonInIdCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		adapter.put("1:a", rand, "persons");

		assertThat(template.hasKey("persons:firstname:rand")).isTrue();

		PartialUpdate<Person> update = new PartialUpdate<>("1:a", Person.class) //
				.set("firstname", "mat");

		adapter.update(update);

		assertThat(template.hasKey("persons:firstname:rand")).isFalse();
		assertThat(template.hasKey("persons:firstname:mat")).isTrue();
	}

	@Test // DATAREDIS-471
	void updateShouldAlterIndexDataOnNestedObjectCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:address.country:andor")).isTrue();

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class);
		Address addressUpdate = new Address();
		addressUpdate.country = "tear";

		update = update.set("address", addressUpdate);

		adapter.update(update);

		assertThat(template.hasKey("persons:address.country:andor")).isFalse();
		assertThat(template.hasKey("persons:address.country:tear")).isTrue();
	}

	@Test // DATAREDIS-471
	void updateShouldAlterIndexDataOnNestedObjectPathCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:address.country:andor")).isTrue();

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.set("address.country", "tear");

		adapter.update(update);

		assertThat(template.hasKey("persons:address.country:andor")).isFalse();
		assertThat(template.hasKey("persons:address.country:tear")).isTrue();
	}

	@Test // DATAREDIS-471
	void updateShouldRemoveComplexObjectCorrectly() {

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";
		rand.address.city = "emond's field";

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("address");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "address.country")).isFalse();
		assertThat(template.opsForHash().hasKey("persons:1", "address.city")).isFalse();
		assertThat(template.opsForSet().isMember("persons:address.country:andor", "1")).isFalse();
	}

	@Test // DATAREDIS-471
	void updateShouldRemoveSimpleListValuesCorrectly() {

		Person rand = new Person();
		rand.nicknames = Arrays.asList("lews therin", "dragon reborn");

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("nicknames");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "nicknames.[0]")).isFalse();
		assertThat(template.opsForHash().hasKey("persons:1", "nicknames.[1]")).isFalse();
	}

	@Test // DATAREDIS-471
	void updateShouldRemoveComplexListValuesCorrectly() {

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

		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[0].firstname")).isFalse();
		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[0].nicknames.[0]")).isFalse();
		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[1].firstname")).isFalse();
		assertThat(template.opsForHash().hasKey("persons:1", "coworkers.[1].nicknames.[0]")).isFalse();
	}

	@Test // DATAREDIS-471
	void updateShouldRemoveSimpleMapValuesCorrectly() {

		Person rand = new Person();
		rand.physicalAttributes = Collections.singletonMap("eye-color", "grey");

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("physicalAttributes");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "physicalAttributes.[eye-color]")).isFalse();
	}

	@Test // DATAREDIS-471
	void updateShouldRemoveComplexMapValuesCorrectly() {

		Person tam = new Person();
		tam.firstname = "tam";

		Person rand = new Person();
		rand.relatives = Collections.singletonMap("stepfather", tam);

		adapter.put("1", rand, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("relatives");

		adapter.update(update);

		assertThat(template.opsForHash().hasKey("persons:1", "relatives.[stepfather].firstname")).isFalse();
	}

	@Test // DATAREDIS-533
	void putShouldCreateGeoIndexCorrectly() {

		Person tam = new Person();
		tam.id = "1";
		tam.firstname = "tam";
		tam.address = new Address();
		tam.address.location = new Point(10, 20);

		adapter.put("1", tam, "persons");

		assertThat(template.opsForZSet().score("persons:address:location", "1")).isNotNull();
	}

	@Test // DATAREDIS-533
	void deleteShouldRemoveGeoIndexCorrectly() {

		Person tam = new Person();
		tam.id = "1";
		tam.firstname = "tam";
		tam.address = new Address();
		tam.address.location = new Point(10, 20);

		adapter.put("1", tam, "persons");

		adapter.delete("1", "persons", Person.class);

		assertThat(template.opsForZSet().score("persons:address:location", "1")).isNull();
	}

	@Test // DATAREDIS-533
	void updateShouldAlterGeoIndexCorrectlyOnDelete() {

		Person tam = new Person();
		tam.id = "1";
		tam.firstname = "tam";
		tam.address = new Address();
		tam.address.location = new Point(10, 20);

		adapter.put("1", tam, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.del("address.location");

		adapter.update(update);

		assertThat(template.opsForZSet().score("persons:address:location", "1")).isNull();
	}

	@Test // DATAREDIS-533, DATAREDIS-614
	void updateShouldAlterGeoIndexCorrectlyOnUpdate() {

		Person tam = new Person();
		tam.id = "1";
		tam.firstname = "tam";
		tam.address = new Address();
		tam.address.location = new Point(10, 20);

		adapter.put("1", tam, "persons");

		PartialUpdate<Person> update = new PartialUpdate<>("1", Person.class) //
				.set("address.location", new Point(17, 18));

		adapter.update(update);

		assertThat(template.opsForZSet().score("persons:address:location", "1")).isNotNull();
		Point updatedLocation = template.opsForGeo().position("persons:address:location", "1").iterator().next();

		assertThat(updatedLocation.getX()).isCloseTo(17D, offset(0.005));
		assertThat(updatedLocation.getY()).isCloseTo(18D, offset(0.005));
	}

	@Test // DATAREDIS-1091
	void phantomKeyNotInsertedOnPutWhenShadowCopyIsTurnedOff() {

		RedisMappingContext mappingContext = new RedisMappingContext(
				new MappingConfiguration(new IndexConfiguration(), new KeyspaceConfiguration()));
		mappingContext.afterPropertiesSet();

		RedisKeyValueAdapter kvAdapter = new RedisKeyValueAdapter(template, mappingContext);
		kvAdapter.setShadowCopy(ShadowCopy.OFF);

		ExpiringPerson rand = new ExpiringPerson();
		rand.age = 24;
		rand.ttl = 3000L;

		kvAdapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:1:phantom")).isFalse();
	}

	@Test // DATAREDIS-1091
	void phantomKeyInsertedOnPutWhenShadowCopyIsTurnedOn() {

		RedisMappingContext mappingContext = new RedisMappingContext(
				new MappingConfiguration(new IndexConfiguration(), new KeyspaceConfiguration()));
		mappingContext.afterPropertiesSet();

		RedisKeyValueAdapter kvAdapter = new RedisKeyValueAdapter(template, mappingContext);
		kvAdapter.setShadowCopy(ShadowCopy.ON);

		ExpiringPerson rand = new ExpiringPerson();
		rand.age = 24;
		rand.ttl = 3000L;

		kvAdapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:1:phantom")).isTrue();
	}

	@Test // DATAREDIS-1091
	void phantomKeyInsertedOnPutWhenShadowCopyIsInDefaultAndKeyspaceNotificationEnabled() {

		ExpiringPerson rand = new ExpiringPerson();
		rand.age = 24;
		rand.ttl = 3000L;

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:1:phantom")).isTrue();
	}

	@Test // GH-1955
	void phantomKeyIsDeletedWhenPutWithNegativeTimeToLiveAndOldEntryTimeToLiveWasPositiveAndWhenShadowCopyIsTurnedOn() {

		ExpiringPerson rand = new ExpiringPerson();
		rand.ttl = 3000L;

		adapter.put("1", rand, "persons");

		assertThat(template.getExpire("persons:1:phantom")).isPositive();

		rand.ttl = -1L;

		adapter.put("1", rand, "persons");

		assertThat(template.hasKey("persons:1:phantom")).isFalse();
	}

	@Test // GH-1955
	void updateWithRefreshTtlAndWithoutPositiveTtlShouldDeletePhantomKey() {

		ExpiringPerson person = new ExpiringPerson();
		person.ttl = 100L;

		adapter.put("1", person, "persons");
		assertThat(template.getExpire("persons:1:phantom")).isPositive();

		PartialUpdate<ExpiringPerson> update = new PartialUpdate<>("1", ExpiringPerson.class) //
				.set("ttl", -1L).refreshTtl(true);

		adapter.update(update);

		assertThat(template.getExpire("persons:1")).isNotPositive();
		assertThat(template.hasKey("persons:1:phantom")).isFalse();
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

	@TypeAlias("taveren")
	static class TaVeren extends Person {

	}

	static class ExpiringPerson extends Person {

		@TimeToLive Long ttl;
	}

	@KeySpace("locations")
	static class Location {

		@Id String id;
		String name;
	}

	@KeySpace("withexpiration")
	@RedisHash(timeToLive = 30)
	static class WithExpiration {

		@Id String id;
		String value;
	}

}
