/*
 * Copyright 2015-2022 the original author or authors.
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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.With;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import org.springframework.data.annotation.Id;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.extension.JedisConnectionFactoryExtension;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.extension.LettuceConnectionFactoryExtension;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.redis.test.extension.RedisStanalone;
import org.springframework.data.redis.test.extension.parametrized.MethodSource;
import org.springframework.data.redis.test.extension.parametrized.ParameterizedRedisTest;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Integration tests for {@link RedisKeyValueTemplate}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@MethodSource("params")
public class RedisKeyValueTemplateTests {

	private RedisConnectionFactory connectionFactory;
	private RedisKeyValueTemplate template;
	private RedisTemplate<Object, Object> nativeTemplate;
	private RedisMappingContext context;
	private RedisKeyValueAdapter adapter;

	public RedisKeyValueTemplateTests(RedisConnectionFactory connectionFactory) {
		this.connectionFactory = connectionFactory;
	}

	public static List<RedisConnectionFactory> params() {

		JedisConnectionFactory jedis = JedisConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class);
		LettuceConnectionFactory lettuce = LettuceConnectionFactoryExtension.getConnectionFactory(RedisStanalone.class);

		return Arrays.asList(jedis, lettuce);
	}

	@BeforeEach
	void setUp() {

		nativeTemplate = new RedisTemplate<>();
		nativeTemplate.setConnectionFactory(connectionFactory);
		nativeTemplate.afterPropertiesSet();

		context = new RedisMappingContext();
		adapter = new RedisKeyValueAdapter(nativeTemplate, context);
		template = new RedisKeyValueTemplate(adapter, context);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			connection.flushDb();
			return null;
		});
	}

	@AfterEach
	void tearDown() throws Exception {

		template.destroy();
		adapter.destroy();
	}

	@ParameterizedRedisTest // DATAREDIS-425
	void savesObjectCorrectly() {

		final Person rand = new Person();
		rand.firstname = "rand";

		template.insert(rand);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists(("template-test-person:" + rand.id).getBytes())).isTrue();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-425
	void findProcessesCallbackReturningSingleIdCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		final Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(connection -> mat.id.getBytes(), Person.class);

		assertThat(result.size()).isEqualTo(1);
		assertThat(result).contains(mat);
	}

	@ParameterizedRedisTest // DATAREDIS-425
	void findProcessesCallbackReturningMultipleIdsCorrectly() {

		final Person rand = new Person();
		rand.firstname = "rand";

		final Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(connection -> Arrays.asList(rand.id.getBytes(), mat.id.getBytes()),
				Person.class);

		assertThat(result.size()).isEqualTo(2);
		assertThat(result).contains(rand, mat);
	}

	@ParameterizedRedisTest // DATAREDIS-425
	void findProcessesCallbackReturningNullCorrectly() {

		Person rand = new Person();
		rand.firstname = "rand";

		Person mat = new Person();
		mat.firstname = "mat";

		template.insert(rand);
		template.insert(mat);

		List<Person> result = template.find(connection -> null, Person.class);

		assertThat(result.size()).isEqualTo(0);
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdate() {

		final Person rand = new Person();
		rand.firstname = "rand";

		template.insert(rand);

		/*
		 * Set the lastname and make sure we've an index on it afterwards
		 */
		Person update1 = new Person(rand.id, null, "al-thor");
		PartialUpdate<Person> update = new PartialUpdate<>(rand.id, update1);

		template.insert(update);

		assertThat(template.findById(rand.id, Person.class)).isEqualTo(Optional.of(new Person(rand.id, "rand", "al-thor")));
		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.hGet(("template-test-person:" + rand.id).getBytes(), "firstname".getBytes()))
					.isEqualTo("rand".getBytes());
			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes())).isTrue();
			assertThat(connection.sIsMember("template-test-person:lastname:al-thor".getBytes(), rand.id.getBytes())).isTrue();
			return null;
		});

		/*
		 * Set the firstname and make sure lastname index and value is not affected
		 */
		update = new PartialUpdate<>(rand.id, Person.class).set("firstname", "frodo");

		template.update(update);

		assertThat(template.findById(rand.id, Person.class))
				.isEqualTo(Optional.of(new Person(rand.id, "frodo", "al-thor")));
		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes())).isTrue();
			assertThat(connection.sIsMember("template-test-person:lastname:al-thor".getBytes(), rand.id.getBytes())).isTrue();
			return null;
		});

		/*
		 * Remote firstname and update lastname. Make sure lastname index is updated
		 */
		update = new PartialUpdate<>(rand.id, Person.class) //
				.del("firstname").set("lastname", "baggins");

		template.doPartialUpdate(update);

		assertThat(template.findById(rand.id, Person.class)).isEqualTo(Optional.of(new Person(rand.id, null, "baggins")));
		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes())).isFalse();
			assertThat(connection.exists("template-test-person:lastname:baggins".getBytes())).isTrue();
			assertThat(connection.sIsMember("template-test-person:lastname:baggins".getBytes(), rand.id.getBytes())).isTrue();
			return null;
		});

		/*
		 * Remove lastname and make sure the index vanishes
		 */
		update = new PartialUpdate<>(rand.id, Person.class) //
				.del("lastname");

		template.doPartialUpdate(update);

		assertThat(template.findById(rand.id, Person.class)).isEqualTo(Optional.of(new Person(rand.id, null, null)));
		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.keys("template-test-person:lastname:*".getBytes()).size()).isEqualTo(0);
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdateSimpleType() {

		final VariousTypes source = new VariousTypes();
		source.stringValue = "some-value";

		template.insert(source);

		PartialUpdate<VariousTypes> update = new PartialUpdate<>(source.id, VariousTypes.class) //
				.set("stringValue", "hooya");

		template.doPartialUpdate(update);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "stringValue".getBytes()))
					.isEqualTo("hooya".getBytes());
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"simpleTypedMap._class".getBytes())).isFalse();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdateComplexType() {

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
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "complexValue.name".getBytes()))
							.isEqualTo("Portal Stone".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexValue.dimension.height".getBytes())).isEqualTo("350".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexValue.dimension.width".getBytes())).isEqualTo("70".getBytes());

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexValue.dimension.length".getBytes())).isFalse();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdateObjectType() {

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
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "objectValue._class".getBytes()))
							.isEqualTo(Item.class.getName().getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "objectValue.name".getBytes()))
					.isEqualTo("Portal Stone".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"objectValue.dimension.height".getBytes())).isEqualTo("350".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"objectValue.dimension.width".getBytes())).isEqualTo("70".getBytes());

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(), "objectValue".getBytes()))
					.isFalse();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdateSimpleTypedMap() {

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
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedMap.[spring]".getBytes()))
							.isEqualTo("data".getBytes());
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"simpleTypedMap.[key-1]".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"simpleTypedMap.[key-2]".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"simpleTypedMap.[key-2]".getBytes())).isFalse();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdateComplexTypedMap() {

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
					"complexTypedMap.[horn-of-valere].name".getBytes())).isEqualTo("Horn of Valere".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[horn-of-valere].dimension.height".getBytes())).isEqualTo("70".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[horn-of-valere].dimension.width".getBytes())).isEqualTo("25".getBytes());

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[callandor].name".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[callandor].dimension.height".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[callandor].dimension.width".getBytes())).isFalse();

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[portal-stone].name".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[portal-stone].dimension.height".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[portal-stone].dimension.width".getBytes())).isFalse();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdateObjectTypedMap() {

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
					"untypedMap.[horn-of-valere].name".getBytes())).isEqualTo("Horn of Valere".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[horn-of-valere].dimension.height".getBytes())).isEqualTo("70".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[horn-of-valere].dimension.width".getBytes())).isEqualTo("25".getBytes());

			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[spring]._class".getBytes())).isEqualTo("java.lang.String".getBytes());
			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedMap.[spring]".getBytes()))
							.isEqualTo("data".getBytes());

			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[some-number]._class".getBytes())).isEqualTo("java.lang.Long".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[some-number]".getBytes())).isEqualTo("100".getBytes());

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[callandor].name".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[callandor].dimension.height".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[callandor].dimension.width".getBytes())).isFalse();

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[portal-stone].name".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[portal-stone].dimension.height".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedMap.[portal-stone].dimension.width".getBytes())).isFalse();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdateSimpleTypedList() {

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
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedList.[0]".getBytes()))
							.isEqualTo("spring".getBytes());
			assertThat(
					connection.hExists(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedList.[1]".getBytes()))
							.isFalse();
			assertThat(
					connection.hExists(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedList.[2]".getBytes()))
							.isFalse();
			assertThat(
					connection.hExists(("template-test-type-mapping:" + source.id).getBytes(), "simpleTypedList.[3]".getBytes()))
							.isFalse();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdateComplexTypedList() {

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
					"complexTypedList.[0].name".getBytes())).isEqualTo("Horn of Valere".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedList.[0].dimension.height".getBytes())).isEqualTo("70".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedList.[0].dimension.width".getBytes())).isEqualTo("25".getBytes());

			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[1].name".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[1].dimension.height".getBytes())).isFalse();
			assertThat(connection.hExists(("template-test-type-mapping:" + source.id).getBytes(),
					"complexTypedMap.[1].dimension.width".getBytes())).isFalse();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-471
	void partialUpdateObjectTypedList() {

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
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[0]._class".getBytes()))
							.isEqualTo("java.lang.String".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[0]".getBytes()))
					.isEqualTo("spring".getBytes());

			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[1].name".getBytes()))
							.isEqualTo("Horn of Valere".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedList.[1].dimension.height".getBytes())).isEqualTo("70".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(),
					"untypedList.[1].dimension.width".getBytes())).isEqualTo("25".getBytes());

			assertThat(
					connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[2]._class".getBytes()))
							.isEqualTo("java.lang.Long".getBytes());
			assertThat(connection.hGet(("template-test-type-mapping:" + source.id).getBytes(), "untypedList.[2]".getBytes()))
					.isEqualTo("100".getBytes());

			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-530
	void partialUpdateShouldLeaveIndexesNotInvolvedInUpdateUntouched() {

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

			assertThat(connection.hGet(("template-test-person:" + rand.id).getBytes(), "lastname".getBytes()))
					.isEqualTo("doe".getBytes());
			assertThat(connection.exists("template-test-person:email:rand@twof.com".getBytes())).isTrue();
			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes())).isFalse();
			assertThat(connection.sIsMember("template-test-person:lastname:doe".getBytes(), rand.id.getBytes())).isTrue();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-530
	void updateShouldAlterIndexesCorrectlyWhenValuesGetRemovedFromHash() {

		final Person rand = new Person();
		rand.firstname = "rand";
		rand.lastname = "al-thor";
		rand.email = "rand@twof.com";

		template.insert(rand);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists("template-test-person:email:rand@twof.com".getBytes())).isTrue();
			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes())).isTrue();
			return null;
		});

		rand.lastname = null;

		template.update(rand);

		nativeTemplate.execute((RedisCallback<Void>) connection -> {

			assertThat(connection.exists("template-test-person:email:rand@twof.com".getBytes())).isTrue();
			assertThat(connection.exists("template-test-person:lastname:al-thor".getBytes())).isFalse();
			return null;
		});
	}

	@ParameterizedRedisTest // DATAREDIS-523
	void shouldReadBackExplicitTimeToLive() throws InterruptedException {

		WithTtl source = new WithTtl();
		source.id = "ttl-1";
		source.ttl = 5L;
		source.value = "5 seconds";

		template.insert(source);

		Optional<WithTtl> target = template.findById(source.id, WithTtl.class);
		assertThat(target.get().ttl).isGreaterThan(0L);
	}

	@ParameterizedRedisTest // DATAREDIS-523
	void shouldReadBackExplicitTimeToLiveToPrimitiveField() throws InterruptedException {

		WithPrimitiveTtl source = new WithPrimitiveTtl();
		source.id = "ttl-1";
		source.ttl = 5;
		source.value = "5 seconds";

		template.insert(source);

		Optional<WithPrimitiveTtl> target = template.findById(source.id, WithPrimitiveTtl.class);
		assertThat(target.get().ttl).isGreaterThan(0);
	}

	@ParameterizedRedisTest // DATAREDIS-523
	void shouldReadBackExplicitTimeToLiveWhenFetchingList() throws InterruptedException {

		WithTtl source = new WithTtl();
		source.id = "ttl-1";
		source.ttl = 5L;
		source.value = "5 seconds";

		template.insert(source);

		WithTtl target = template.findAll(WithTtl.class).iterator().next();

		assertThat(target.ttl).isNotNull();
		assertThat(target.ttl).isGreaterThan(0);
	}

	@ParameterizedRedisTest // DATAREDIS-523
	void shouldReadBackExplicitTimeToLiveAndSetItToMinusOnelIfPersisted() throws InterruptedException {

		WithTtl source = new WithTtl();
		source.id = "ttl-1";
		source.ttl = 5L;
		source.value = "5 seconds";

		template.insert(source);

		nativeTemplate.execute(
				(RedisCallback<Boolean>) connection -> connection.persist((WithTtl.class.getName() + ":ttl-1").getBytes()));

		Optional<WithTtl> target = template.findById(source.id, WithTtl.class);
		assertThat(target.get().ttl).isEqualTo(-1L);
	}

	@ParameterizedRedisTest // DATAREDIS-849
	void shouldWriteImmutableType() {

		ImmutableObject source = new ImmutableObject().withValue("foo").withTtl(1234L);

		ImmutableObject inserted = template.insert(source);

		assertThat(source.id).isNull();
		assertThat(inserted.id).isNotNull();
	}

	@ParameterizedRedisTest // DATAREDIS-849
	void shouldReadImmutableType() {

		ImmutableObject source = new ImmutableObject().withValue("foo").withTtl(1234L);
		ImmutableObject inserted = template.insert(source);

		Optional<ImmutableObject> loaded = template.findById(inserted.id, ImmutableObject.class);

		assertThat(loaded.isPresent()).isTrue();

		ImmutableObject immutableObject = loaded.get();
		assertThat(immutableObject.id).isEqualTo(inserted.id);
		assertThat(immutableObject.ttl).isNotNull();
		assertThat(immutableObject.value).isEqualTo(inserted.value);
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

		Person() {}

		public Person(String firstname, String lastname) {
			this(null, firstname, lastname, null);
		}

		Person(String id, String firstname, String lastname) {
			this(id, firstname, lastname, null);
		}

		Person(String id, String firstname, String lastname, Integer age) {

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
		public boolean equals(@Nullable Object obj) {
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
	@With
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
