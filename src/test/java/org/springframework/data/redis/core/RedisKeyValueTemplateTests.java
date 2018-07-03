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

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.number.IsCloseTo.*;
import static org.junit.Assert.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Wither;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.annotation.Id;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceTestClientResources;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.util.ObjectUtils;

/**
 * Integration tests for {@link RedisKeyValueTemplate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
public class RedisKeyValueTemplateTests {

	RedisConnectionFactory connectionFactory;
	RedisKeyValueTemplate template;
	RedisTemplate<Object, Object> nativeTemplate;
	RedisMappingContext context;
	RedisKeyValueAdapter adapter;

	public RedisKeyValueTemplateTests(RedisConnectionFactory connectionFactory) {

		this.connectionFactory = connectionFactory;
		ConnectionFactoryTracker.add(connectionFactory);

	}

	@Parameters
	public static List<RedisConnectionFactory> params() {

		JedisConnectionFactory jedis = new JedisConnectionFactory();
		jedis.afterPropertiesSet();

		LettuceConnectionFactory lettuce = new LettuceConnectionFactory();
		lettuce.setClientResources(LettuceTestClientResources.getSharedClientResources());
		lettuce.afterPropertiesSet();

		return Arrays.<RedisConnectionFactory> asList(jedis, lettuce);
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {

		nativeTemplate = new RedisTemplate<>();
		nativeTemplate.setConnectionFactory(connectionFactory);
		nativeTemplate.afterPropertiesSet();

		context = new RedisMappingContext();
		adapter = new RedisKeyValueAdapter(nativeTemplate, context);
		template = new RedisKeyValueTemplate(adapter, context);
	}

	@After
	public void tearDown() throws Exception {

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			connection.flushDb();
			return null;
		});

		template.destroy();
		adapter.destroy();
	}

	@Test // DATAREDIS-425
	public void savesObjectCorrectly() {

		final Person rand = new Person();
		rand.firstname = "rand";

		template.insert(rand);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists(("template-test-person:" + rand.id).getBytes()), is(true));
			return null;
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

		List<Person> result = template.find(connection -> mat.id.getBytes(), Person.class);

		assertThat(result.size(), is(1));
		assertThat(result, hasItems(mat));
	}

	@Test // DATAREDIS-425
	public void findProcessesCallbackReturningMultipleIdsCorrectly() {

		final Person rand = new Person();
		rand.firstname = "rand";

		final Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(connection -> Arrays.asList(rand.id.getBytes(), mat.id.getBytes()),
				Person.class);

		assertThat(result.size(), is(2));
		assertThat(result, hasItems(rand, mat));
	}

	@Test // DATAREDIS-425
	public void findProcessesCallbackReturningNullCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(connection -> null, Person.class);

		assertThat(result.size(), is(0));
	}

	@Test // DATAREDIS-471
	public void partialUpdate() {

		final Person rand = new Person();
		rand.firstname = "rand";

		template.insert(rand);

		/*
		 * Set the lastname and make sure we've an index on it afterwards
		 */
		Person update1 = new Person(rand.id, null, "al-thor");
		PartialUpdate<Person> update = new PartialUpdate<>(rand.id, update1);

		template.insert(update);

		assertThat(template.findById(rand.id, Person.class), is(equalTo(Optional.of(new Person(rand.id, "rand", "al-thor")))));
		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.hGet(("template-test-person:" + rand.id).getBytes(), "firstname".getBytes()),
					is(equalTo("rand".getBytes())));
			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes()), is(true));
			assertThat(connection.sIsMember("template-test-person:lastname:al-thor".getBytes(), rand.id.getBytes()),
					is(true));
			return null;
		});

		/*
		 * Set the firstname and make sure lastname index and value is not affected
		 */
		update = new PartialUpdate<>(rand.id, Person.class).set("firstname", "frodo");

		template.update(update);

		assertThat(template.findById(rand.id, Person.class), is(equalTo(Optional.of(new Person(rand.id, "frodo", "al-thor")))));
		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes()), is(true));
			assertThat(connection.sIsMember("template-test-person:lastname:al-thor".getBytes(), rand.id.getBytes()),
					is(true));
			return null;
		});

		/*
		 * Remote firstname and update lastname. Make sure lastname index is updated
		 */
		update = new PartialUpdate<>(rand.id, Person.class) //
				.del("firstname").set("lastname", "baggins");

		template.doPartialUpdate(update);

		assertThat(template.findById(rand.id, Person.class), is(equalTo(Optional.of(new Person(rand.id, null, "baggins")))));
		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes()), is(false));
			assertThat(connection.exists("template-test-person:lastname:baggins".getBytes()), is(true));
			assertThat(connection.sIsMember("template-test-person:lastname:baggins".getBytes(), rand.id.getBytes()),
					is(true));
			return null;
		});

		/*
		 * Remove lastname and make sure the index vanishes
		 */
		update = new PartialUpdate<>(rand.id, Person.class) //
				.del("lastname");

		template.doPartialUpdate(update);

		assertThat(template.findById(rand.id, Person.class), is(equalTo(Optional.of(new Person(rand.id, null, null)))));
		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.keys("template-test-person:lastname:*".getBytes()).size(), is(0));
			return null;
		});
	}

	@Test // DATAREDIS-471
	public void partialUpdateSimpleType() {

		final VariousTypes source = new VariousTypes();
		source.stringValue = "some-value";

		template.insert(source);

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("stringValue", "hooya!");

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "stringValue".getBytes()),
					is("hooya!".getBytes()));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"simpleTypedMap._class".getBytes()), is(false));
			return null;
		});
	}

	@Test // DATAREDIS-471
	public void partialUpdateComplexType() {

		Item callandor = new Item();
		callandor.name = "Callandor";
		callandor.dimension = new Dimension();
		callandor.dimension.length = 100;

		final VariousTypes source = new VariousTypes();
		source.complexValue = callandor;

		template.insert(source);

		Item portalStone = new Item();
		portalStone.name = "Portal Stone";
		portalStone.dimension = new Dimension();
		portalStone.dimension.height = 350;
		portalStone.dimension.width = 70;

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("complexValue", portalStone);

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "complexValue.name".getBytes()),
					is("Portal Stone".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexValue.dimension.height".getBytes()), is("350".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexValue.dimension.width".getBytes()), is("70".getBytes()));

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexValue.dimension.length".getBytes()), is(false));
			return null;
		});
	}

	@Test // DATAREDIS-471
	public void partialUpdateObjectType() {

		Item callandor = new Item();
		callandor.name = "Callandor";
		callandor.dimension = new Dimension();
		callandor.dimension.length = 100;

		final VariousTypes source = new VariousTypes();
		source.objectValue = callandor;

		template.insert(source);

		Item portalStone = new Item();
		portalStone.name = "Portal Stone";
		portalStone.dimension = new Dimension();
		portalStone.dimension.height = 350;
		portalStone.dimension.width = 70;

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("objectValue", portalStone);

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "objectValue._class".getBytes()),
					is(Item.class.getName().getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "objectValue.name".getBytes()),
					is("Portal Stone".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"objectValue.dimension.height".getBytes()), is("350".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"objectValue.dimension.width".getBytes()), is("70".getBytes()));

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(), "objectValue".getBytes()),
					is(false));
			return null;
		});
	}

	@Test // DATAREDIS-471
	public void partialUpdateSimpleTypedMap() {

		final VariousTypes source = new VariousTypes();
		source.simpleTypedMap = new LinkedHashMap<>();
		source.simpleTypedMap.put("key-1", "rand");
		source.simpleTypedMap.put("key-2", "mat");
		source.simpleTypedMap.put("key-3", "perrin");

		template.insert(source);

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("simpleTypedMap", Collections.singletonMap("spring", "data"));

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedMap.[spring]".getBytes()),
					is("data".getBytes()));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"simpleTypedMap.[key-1]".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"simpleTypedMap.[key-2]".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"simpleTypedMap.[key-2]".getBytes()), is(false));
			return null;
		});
	}

	@Test // DATAREDIS-471
	public void partialUpdateComplexTypedMap() {

		final VariousTypes source = new VariousTypes();
		source.complexTypedMap = new LinkedHashMap<>();

		Item callandor = new Item();
		callandor.name = "Callandor";
		callandor.dimension = new Dimension();
		callandor.dimension.height = 100;

		Item portalStone = new Item();
		portalStone.name = "Portal Stone";
		portalStone.dimension = new Dimension();
		portalStone.dimension.height = 350;
		portalStone.dimension.width = 70;

		source.complexTypedMap.put("callandor", callandor);
		source.complexTypedMap.put("portal-stone", portalStone);

		template.insert(source);

		Item hornOfValere = new Item();
		hornOfValere.name = "Horn of Valere";
		hornOfValere.dimension = new Dimension();
		hornOfValere.dimension.height = 70;
		hornOfValere.dimension.width = 25;

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("complexTypedMap", Collections.singletonMap("horn-of-valere", hornOfValere));

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[horn-of-valere].name".getBytes()), is("Horn of Valere".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[horn-of-valere].dimension.height".getBytes()), is("70".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[horn-of-valere].dimension.width".getBytes()), is("25".getBytes()));

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[callandor].name".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[callandor].dimension.height".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[callandor].dimension.width".getBytes()), is(false));

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[portal-stone].name".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[portal-stone].dimension.height".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[portal-stone].dimension.width".getBytes()), is(false));
			return null;
		});
	}

	@Test // DATAREDIS-471
	public void partialUpdateObjectTypedMap() {

		final VariousTypes source = new VariousTypes();
		source.untypedMap = new LinkedHashMap<>();

		Item callandor = new Item();
		callandor.name = "Callandor";
		callandor.dimension = new Dimension();
		callandor.dimension.height = 100;

		Item portalStone = new Item();
		portalStone.name = "Portal Stone";
		portalStone.dimension = new Dimension();
		portalStone.dimension.height = 350;
		portalStone.dimension.width = 70;

		source.untypedMap.put("callandor", callandor);
		source.untypedMap.put("just-a-string", "some-string-value");
		source.untypedMap.put("portal-stone", portalStone);

		template.insert(source);

		Item hornOfValere = new Item();
		hornOfValere.name = "Horn of Valere";
		hornOfValere.dimension = new Dimension();
		hornOfValere.dimension.height = 70;
		hornOfValere.dimension.width = 25;

		Map<String, Object> map = new LinkedHashMap<>();
		map.put("spring", "data");
		map.put("horn-of-valere", hornOfValere);
		map.put("some-number", 100L);

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("untypedMap", map);

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[horn-of-valere].name".getBytes()), is("Horn of Valere".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[horn-of-valere].dimension.height".getBytes()), is("70".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[horn-of-valere].dimension.width".getBytes()), is("25".getBytes()));

			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[spring]._class".getBytes()), is("java.lang.String".getBytes()));
			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedMap.[spring]".getBytes()),
					is("data".getBytes()));

			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[some-number]._class".getBytes()), is("java.lang.Long".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[some-number]".getBytes()), is("100".getBytes()));

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[callandor].name".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[callandor].dimension.height".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[callandor].dimension.width".getBytes()), is(false));

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[portal-stone].name".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[portal-stone].dimension.height".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[portal-stone].dimension.width".getBytes()), is(false));
			return null;
		});
	}

	@Test // DATAREDIS-471
	public void partialUpdateSimpleTypedList() {

		final VariousTypes source = new VariousTypes();
		source.simpleTypedList = new ArrayList<>();
		source.simpleTypedList.add("rand");
		source.simpleTypedList.add("mat");
		source.simpleTypedList.add("perrin");

		template.insert(source);

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("simpleTypedList", Collections.singletonList("spring"));

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedList.[0]".getBytes()),
					is("spring".getBytes()));
			assertThat(
					connection.hExists(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedList.[1]".getBytes()),
					is(false));
			assertThat(
					connection.hExists(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedList.[2]".getBytes()),
					is(false));
			assertThat(
					connection.hExists(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedList.[3]".getBytes()),
					is(false));
			return null;
		});
	}

	@Test // DATAREDIS-471
	public void partialUpdateComplexTypedList() {

		final VariousTypes source = new VariousTypes();
		source.complexTypedList = new ArrayList<>();

		Item callandor = new Item();
		callandor.name = "Callandor";
		callandor.dimension = new Dimension();
		callandor.dimension.height = 100;

		Item portalStone = new Item();
		portalStone.name = "Portal Stone";
		portalStone.dimension = new Dimension();
		portalStone.dimension.height = 350;
		portalStone.dimension.width = 70;

		source.complexTypedList.add(callandor);
		source.complexTypedList.add(portalStone);

		template.insert(source);

		Item hornOfValere = new Item();
		hornOfValere.name = "Horn of Valere";
		hornOfValere.dimension = new Dimension();
		hornOfValere.dimension.height = 70;
		hornOfValere.dimension.width = 25;

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("complexTypedList", Collections.singletonList(hornOfValere));

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedList.[0].name".getBytes()), is("Horn of Valere".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedList.[0].dimension.height".getBytes()), is("70".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedList.[0].dimension.width".getBytes()), is("25".getBytes()));

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[1].name".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[1].dimension.height".getBytes()), is(false));
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[1].dimension.width".getBytes()), is(false));
			return null;
		});
	}

	@Test // DATAREDIS-471
	public void partialUpdateObjectTypedList() {

		final VariousTypes source = new VariousTypes();
		source.untypedList = new ArrayList<>();

		Item callandor = new Item();
		callandor.name = "Callandor";
		callandor.dimension = new Dimension();
		callandor.dimension.height = 100;

		Item portalStone = new Item();
		portalStone.name = "Portal Stone";
		portalStone.dimension = new Dimension();
		portalStone.dimension.height = 350;
		portalStone.dimension.width = 70;

		source.untypedList.add(callandor);
		source.untypedList.add("some-string-value");
		source.untypedList.add(portalStone);

		template.insert(source);

		Item hornOfValere = new Item();
		hornOfValere.name = "Horn of Valere";
		hornOfValere.dimension = new Dimension();
		hornOfValere.dimension.height = 70;
		hornOfValere.dimension.width = 25;

		List<Object> list = new ArrayList<>();
		list.add("spring");
		list.add(hornOfValere);
		list.add(100L);

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("untypedList", list);

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[0]._class".getBytes()),
					is("java.lang.String".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[0]".getBytes()),
					is("spring".getBytes()));

			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[1].name".getBytes()),
					is("Horn of Valere".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedList.[1].dimension.height".getBytes()), is("70".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedList.[1].dimension.width".getBytes()), is("25".getBytes()));

			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[2]._class".getBytes()),
					is("java.lang.Long".getBytes()));
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[2]".getBytes()),
					is("100".getBytes()));

			return null;
		});
	}

	@Test // DATAREDIS-530
	public void partialUpdateShouldLeaveIndexesNotInvolvedInUpdateUntouched() {

		final Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al-thor";
		rand.email = "rand@twof.com";

		template.insert(rand);

		/*
		 * Set the lastname and make sure we've an index on it afterwards
		 */
		PartialUpdate<Person> update = PartialUpdate.newPartialUpdate(rand.id, Person.class).set("lastname", "doe");

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.hGet(("template-test-person:" + rand.id).getBytes(), "lastname".getBytes()),
					is(equalTo("doe".getBytes())));
			assertThat(connection.exists("template-test-person:email:rand@twof.com".getBytes()), is(true));
			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes()), is(false));
			assertThat(connection.sIsMember("template-test-person:lastname:doe".getBytes(), rand.id.getBytes()), is(true));
			return null;
		});
	}

	@Test // DATAREDIS-530
	public void updateShouldAlterIndexesCorrectlyWhenValuesGetRemovedFromHash() {

		final Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al-thor";
		rand.email = "rand@twof.com";

		template.insert(rand);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists("template-test-person:email:rand@twof.com".getBytes()), is(true));
			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes()), is(true));
			return null;
		});

		rand.lastname = null;

		template.update(rand);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists("template-test-person:email:rand@twof.com".getBytes()), is(true));
			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes()), is(false));
			return null;
		});
	}

	@Test // DATAREDIS-523
	public void shouldReadBackExplicitTimeToLive() throws InterruptedException {

		WithTtl source = new WithTtl();
		source.id = "ttl-1";
		source.ttl = 5L;
		source.value = "5 seconds";

		template.insert(source);

		Thread.sleep(1100);

		Optional<WithTtl> target = template.findById(source.id, WithTtl.class);
		assertThat(target.get().ttl, is(notNullValue()));
		assertThat(target.get().ttl.doubleValue(), is(closeTo(3D, 1D)));
	}

	@Test // DATAREDIS-523
	public void shouldReadBackExplicitTimeToLiveToPrimitiveField() throws InterruptedException {

		WithPrimitiveTtl source = new WithPrimitiveTtl();
		source.id = "ttl-1";
		source.ttl = 5;
		source.value = "5 seconds";

		template.insert(source);

		Thread.sleep(1100);

		Optional<WithPrimitiveTtl> target = template.findById(source.id, WithPrimitiveTtl.class);
		assertThat((double) target.get().ttl, is(closeTo(3D, 1D)));
	}

	@Test // DATAREDIS-523
	public void shouldReadBackExplicitTimeToLiveWhenFetchingList() throws InterruptedException {

		WithTtl source = new WithTtl();
		source.id = "ttl-1";
		source.ttl = 5L;
		source.value = "5 seconds";

		template.insert(source);

		Thread.sleep(1100);

		WithTtl target = template.findAll(WithTtl.class).iterator().next();

		assertThat(target.ttl, is(notNullValue()));
		assertThat(target.ttl.doubleValue(), is(closeTo(3D, 1D)));
	}

	@Test // DATAREDIS-523
	public void shouldReadBackExplicitTimeToLiveAndSetItToMinusOnelIfPersisted() throws InterruptedException {

		WithTtl source = new WithTtl();
		source.id = "ttl-1";
		source.ttl = 5L;
		source.value = "5 seconds";

		template.insert(source);

		nativeTemplate.execute(
				(RedisCallback<Boolean>) connection -> connection.persist((WithTtl.class.getName() + ":ttl-1").getBytes()));

		Optional<WithTtl> target = template.findById(source.id, WithTtl.class);
		assertThat(target.get().ttl, is(-1L));
	}

	@Test // DATAREDIS-849
	public void shouldWriteImmutableType() {

		ImmutableObject source = new ImmutableObject().withValue("foo").withTtl(1234L);

		ImmutableObject inserted = template.insert(source);

		assertThat(source.id, is(nullValue()));
		assertThat(inserted.id, is(notNullValue()));
	}

	@Test // DATAREDIS-849
	public void shouldReadImmutableType() {

		ImmutableObject source = new ImmutableObject().withValue("foo").withTtl(1234L);
		ImmutableObject inserted = template.insert(source);

		Optional<ImmutableObject> loaded = template.findById(inserted.id, ImmutableObject.class);

		assertThat(loaded.isPresent(), is(true));

		ImmutableObject immutableObject = loaded.get();
		assertThat(immutableObject.id, is(inserted.id));
		assertThat(immutableObject.ttl, is(notNullValue()));
		assertThat(immutableObject.value, is(inserted.value));
	}

	@EqualsAndHashCode
	@RedisHash("template-test-type-mapping")
	static class VariousTypes {

		@Id String id;

		String stringValue;
		Integer integerValue;
		Item complexValue;
		Object objectValue;

		List<String> simpleTypedList;
		List<Item> complexTypedList;
		List<Object> untypedList;

		Map<String, String> simpleTypedMap;
		Map<String, Item> complexTypedMap;
		Map<String, Object> untypedMap;
	}

	static class Item {
		String name;
		Dimension dimension;
	}

	static class Dimension {

		Integer height;
		Integer width;
		Integer length;
	}

	@RedisHash("template-test-person")
	static class Person {

		@Id String id;
		String firstname;
		@Indexed String lastname;
		@Indexed String email;
		Integer age;
		List<String> nicknames;

		public Person() {}

		public Person(String firstname, String lastname) {
			this(null, firstname, lastname, null);
		}

		public Person(String id, String firstname, String lastname) {
			this(id, firstname, lastname, null);
		}

		public Person(String id, String firstname, String lastname, Integer age) {

			this.id = id;
			this.firstname = firstname;
			this.lastname = lastname;
			this.age = age;
		}

		@Override
		public int hashCode() {

			int result = ObjectUtils.nullSafeHashCode(firstname);
			result += ObjectUtils.nullSafeHashCode(lastname);
			result += ObjectUtils.nullSafeHashCode(age);
			result += ObjectUtils.nullSafeHashCode(nicknames);
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

			if (!ObjectUtils.nullSafeEquals(this.lastname, that.lastname)) {
				return false;
			}

			if (!ObjectUtils.nullSafeEquals(this.age, that.age)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(this.nicknames, that.nicknames)) {
				return false;
			}

			return ObjectUtils.nullSafeEquals(this.id, that.id);
		}

		@Override
		public String toString() {
			return "Person [id=" + id + ", firstname=" + firstname + ", lastname=" + lastname + ", age=" + age
					+ ", nicknames=" + nicknames + "]";
		}

	}

	@Data
	static class WithTtl {

		@Id String id;
		String value;
		@TimeToLive Long ttl;
	}

	@Data
	static class WithPrimitiveTtl {

		@Id String id;
		String value;
		@TimeToLive int ttl;
	}

	@Data
	@Wither
	@AllArgsConstructor
	static class ImmutableObject {

		final @Id String id;
		final String value;
		final @TimeToLive Long ttl;

		ImmutableObject() {
			this.id = null;
			this.value = null;
			this.ttl = null;
		}
	}
}
