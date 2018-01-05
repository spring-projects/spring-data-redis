/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.redis.mapping;

import lombok.Data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.Address;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.hash.HashMapper;
import org.springframework.data.redis.hash.Jackson2HashMapper;

/**
 * Unit tests for {@link Jackson2HashMapper}.
 *
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
public class Jackson2HashMapperUnitTests extends AbstractHashMapperTest {

	private final Jackson2HashMapper mapper;

	public Jackson2HashMapperUnitTests(Jackson2HashMapper mapper) {
		this.mapper = mapper;
	}

	@Parameters
	public static Collection<Jackson2HashMapper> params() {
		return Arrays.asList(new Jackson2HashMapper(true), new Jackson2HashMapper(false));
	}

	@Override
	@SuppressWarnings("rawtypes")
	protected <T> HashMapper mapperFor(Class<T> t) {
		return this.mapper;
	}

	@Test // DATAREDIS-423
	public void shouldMapTypedListOfSimpleType() {

		WithList source = new WithList();
		source.strings = Arrays.asList("spring", "data", "redis");
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	public void shouldMapTypedListOfComplexType() {

		WithList source = new WithList();

		source.persons = Arrays.asList(new Person("jon", "snow", 19), new Person("tyrion", "lannister", 27));
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	public void shouldMapTypedListOfComplexObjectWihtNestedElements() {

		WithList source = new WithList();

		Person jon = new Person("jon", "snow", 19);
		Address adr = new Address();
		adr.setStreet("the wall");
		adr.setNumber(100);
		jon.setAddress(adr);

		source.persons = Arrays.asList(jon, new Person("tyrion", "lannister", 27));
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	public void shouldMapNestedObject() {

		Person jon = new Person("jon", "snow", 19);
		Address adr = new Address();
		adr.setStreet("the wall");
		adr.setNumber(100);
		jon.setAddress(adr);

		assertBackAndForwardMapping(jon);
	}

	@Test // DATAREDIS-423
	public void shouldMapUntypedList() {

		WithList source = new WithList();
		source.objects = Arrays.<Object> asList(Integer.valueOf(100), "foo", new Person("jon", "snow", 19));
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	public void shouldMapTypedMapOfSimpleTypes() {

		WithMap source = new WithMap();
		source.strings = new LinkedHashMap<>();
		source.strings.put("1", "spring");
		source.strings.put("2", "data");
		source.strings.put("3", "redis");
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	public void shouldMapTypedMapOfComplexTypes() {

		WithMap source = new WithMap();
		source.persons = new LinkedHashMap<>();
		source.persons.put("1", new Person("jon", "snow", 19));
		source.persons.put("2", new Person("tyrion", "lannister", 19));
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	public void shouldMapUntypedMap() {

		WithMap source = new WithMap();
		source.objects = new LinkedHashMap<>();
		source.objects.put("1", "spring");
		source.objects.put("2", Integer.valueOf(100));
		source.objects.put("3", "redis");
		assertBackAndForwardMapping(source);
	}

	@Test // DATAREDIS-423
	public void nestedStuff() {

		WithList nestedList = new WithList();
		nestedList.objects = new ArrayList<>();

		WithMap deepNestedMap = new WithMap();
		deepNestedMap.persons = new LinkedHashMap<>();
		deepNestedMap.persons.put("jon", new Person("jon", "snow", 24));

		nestedList.objects.add(deepNestedMap);

		WithMap outer = new WithMap();
		outer.objects = new LinkedHashMap<>();
		outer.objects.put("1", nestedList);

		assertBackAndForwardMapping(outer);
	}

	@Data
	public static class WithList {
		List<String> strings;
		List<Object> objects;
		List<Person> persons;
	}

	@Data
	public static class WithMap {
		Map<String, String> strings;
		Map<String, Object> objects;
		Map<String, Person> persons;
	}
}
