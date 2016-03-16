/*
 * Copyright 2015-2016 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import static org.hamcrest.collection.IsEmptyCollection.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.data.redis.core.convert.ConversionTestEntities.*;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hamcrest.core.IsCollectionContaining;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Address;
import org.springframework.data.redis.core.convert.ConversionTestEntities.AddressWithId;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Item;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Location;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Person;
import org.springframework.data.redis.core.convert.ConversionTestEntities.PersonWithAddressReference;
import org.springframework.data.redis.core.convert.ConversionTestEntities.Size;
import org.springframework.data.redis.core.convert.ConversionTestEntities.TaVeren;
import org.springframework.data.redis.core.convert.ConversionTestEntities.TheWheelOfTime;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.index.SimpleIndexDefinition;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.util.ClassTypeInformation;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class PathIndexResolverUnitTests {

	IndexConfiguration indexConfig;
	PathIndexResolver indexResolver;

	@Mock PersistentProperty<?> propertyMock;

	@Before
	public void setUp() {

		indexConfig = new IndexConfiguration();
		this.indexResolver = new PathIndexResolver(
				new RedisMappingContext(new MappingConfiguration(indexConfig, new KeyspaceConfiguration())));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test(expected = IllegalArgumentException.class)
	public void shouldThrowExceptionOnNullMappingContext() {
		new PathIndexResolver(null);
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void shouldResolveAnnotatedIndexOnRootWhenValueIsNotNull() {

		Address address = new Address();
		address.country = "andor";

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Address.class), address);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(Address.class.getName(), "country", "andor")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void shouldNotResolveAnnotatedIndexOnRootWhenValueIsNull() {

		Address address = new Address();
		address.country = null;

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Address.class), address);

		assertThat(indexes.size(), is(0));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void shouldResolveAnnotatedIndexOnNestedObjectWhenValueIsNotNull() {

		Person person = new Person();
		person.address = new Address();
		person.address.country = "andor";

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Person.class), person);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "address.country", "andor")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void shouldResolveMultipleAnnotatedIndexesInLists() {

		TheWheelOfTime twot = new TheWheelOfTime();
		twot.mainCharacters = new ArrayList<Person>();

		Person rand = new Person();
		rand.address = new Address();
		rand.address.country = "andor";

		Person zarine = new Person();
		zarine.address = new Address();
		zarine.address.country = "saldaea";

		twot.mainCharacters.add(rand);
		twot.mainCharacters.add(zarine);

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TheWheelOfTime.class), twot);

		assertThat(indexes.size(), is(2));
		assertThat(indexes,
				IsCollectionContaining.<IndexedData> hasItems(
						new SimpleIndexedPropertyValue(KEYSPACE_TWOT, "mainCharacters.address.country", "andor"),
						new SimpleIndexedPropertyValue(KEYSPACE_TWOT, "mainCharacters.address.country", "saldaea")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void shouldResolveAnnotatedIndexesInMap() {

		TheWheelOfTime twot = new TheWheelOfTime();
		twot.places = new LinkedHashMap<String, ConversionTestEntities.Location>();

		Location stoneOfTear = new Location();
		stoneOfTear.name = "Stone of Tear";
		stoneOfTear.address = new Address();
		stoneOfTear.address.city = "tear";
		stoneOfTear.address.country = "illian";

		twot.places.put("stone-of-tear", stoneOfTear);

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TheWheelOfTime.class), twot);

		assertThat(indexes.size(), is(1));
		assertThat(indexes,
				hasItem(new SimpleIndexedPropertyValue(KEYSPACE_TWOT, "places.stone-of-tear.address.country", "illian")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void shouldResolveConfiguredIndexesInMapOfSimpleTypes() {

		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "physicalAttributes.eye-color"));

		Person rand = new Person();
		rand.physicalAttributes = new LinkedHashMap<String, String>();
		rand.physicalAttributes.put("eye-color", "grey");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Person.class), rand);

		assertThat(indexes.size(), is(1));
		assertThat(indexes,
				hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "physicalAttributes.eye-color", "grey")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void shouldResolveConfiguredIndexesInMapOfComplexTypes() {

		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "relatives.father.firstname"));

		Person rand = new Person();
		rand.relatives = new LinkedHashMap<String, Person>();

		Person janduin = new Person();
		janduin.firstname = "janduin";

		rand.relatives.put("father", janduin);

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Person.class), rand);

		assertThat(indexes.size(), is(1));
		assertThat(indexes,
				hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "relatives.father.firstname", "janduin")));
	}

	/**
	 * @see DATAREDIS-425
	 * @see DATAREDIS-471
	 */
	@Test
	public void shouldIgnoreConfiguredIndexesInMapWhenValueIsNull() {

		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "physicalAttributes.eye-color"));

		Person rand = new Person();
		rand.physicalAttributes = new LinkedHashMap<String, String>();
		rand.physicalAttributes.put("eye-color", null);

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Person.class), rand);

		assertThat(indexes.size(), is(1));
		assertThat(indexes.iterator().next(), IsInstanceOf.instanceOf(RemoveIndexedData.class));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void shouldNotResolveIndexOnReferencedEntity() {

		PersonWithAddressReference rand = new PersonWithAddressReference();
		rand.addressRef = new AddressWithId();
		rand.addressRef.id = "emond_s_field";
		rand.addressRef.country = "andor";

		Set<IndexedData> indexes = indexResolver
				.resolveIndexesFor(ClassTypeInformation.from(PersonWithAddressReference.class), rand);

		assertThat(indexes.size(), is(0));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldReturnNullWhenNoIndexConfigured() {

		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(false);
		assertThat(resolve("foo", "rand"), nullValue());
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldReturnDataWhenIndexConfigured() {

		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(false);
		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "foo"));

		assertThat(resolve("foo", "rand"), notNullValue());
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldReturnDataWhenNoIndexConfiguredButPropertyAnnotated() {

		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(Indexed.class))).thenReturn(createIndexedInstance());

		assertThat(resolve("foo", "rand"), notNullValue());
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldRemovePositionIndicatorForValuesInLists() {

		when(propertyMock.isCollectionLike()).thenReturn(true);
		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(Indexed.class))).thenReturn(createIndexedInstance());

		IndexedData index = resolve("list.[0].name", "rand");

		assertThat(index.getIndexName(), is("list.name"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldRemoveKeyIndicatorForValuesInMap() {

		when(propertyMock.isMap()).thenReturn(true);
		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(Indexed.class))).thenReturn(createIndexedInstance());

		IndexedData index = resolve("map.[foo].name", "rand");

		assertThat(index.getIndexName(), is("map.foo.name"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldKeepNumericalKeyForValuesInMap() {

		when(propertyMock.isMap()).thenReturn(true);
		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(Indexed.class))).thenReturn(createIndexedInstance());

		IndexedData index = resolve("map.[0].name", "rand");

		assertThat(index.getIndexName(), is("map.0.name"));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldInspectObjectTypeProperties() {

		Item hat = new Item();
		hat.type = "hat";

		TaVeren mat = new TaVeren();
		mat.feature = hat;

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "feature.type", "hat")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldInspectObjectTypePropertiesButIgnoreNullValues() {

		Item hat = new Item();
		hat.description = "wide brimmed hat";

		TaVeren mat = new TaVeren();
		mat.feature = hat;

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(0));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldInspectObjectTypeValuesInMapProperties() {

		Item hat = new Item();
		hat.type = "hat";

		TaVeren mat = new TaVeren();
		mat.characteristics = new LinkedHashMap<String, Object>(2);
		mat.characteristics.put("clothing", hat);
		mat.characteristics.put("gambling", "owns the dark one's luck");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(1));
		assertThat(indexes,
				hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "characteristics.clothing.type", "hat")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexShouldInspectObjectTypeValuesInListProperties() {

		Item hat = new Item();
		hat.type = "hat";

		TaVeren mat = new TaVeren();
		mat.items = new ArrayList<Object>(2);
		mat.items.add(hat);
		mat.items.add("foxhead medallion");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "items.type", "hat")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexAllowCustomIndexName() {

		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "items.type", "itemsType"));

		Item hat = new Item();
		hat.type = "hat";

		TaVeren mat = new TaVeren();
		mat.items = new ArrayList<Object>(2);
		mat.items.add(hat);
		mat.items.add("foxhead medallion");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "itemsType", "hat")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexForTypeThatHasNoIndexDefined() {

		Size size = new Size();
		size.height = 10;
		size.length = 20;
		size.width = 30;

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Size.class), size);
		assertThat(indexes, is(empty()));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexOnMapField() {

		IndexedOnMapField source = new IndexedOnMapField();
		source.values = new LinkedHashMap<String, String>();

		source.values.put("jon", "snow");
		source.values.put("arya", "stark");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(IndexedOnMapField.class),
				source);

		assertThat(indexes.size(), is(2));
		assertThat(indexes,
				IsCollectionContaining.<IndexedData> hasItems(
						new SimpleIndexedPropertyValue(IndexedOnMapField.class.getName(), "values.jon", "snow"),
						new SimpleIndexedPropertyValue(IndexedOnMapField.class.getName(), "values.arya", "stark")));
	}

	/**
	 * @see DATAREDIS-425
	 */
	@Test
	public void resolveIndexOnListField() {

		IndexedOnListField source = new IndexedOnListField();
		source.values = new ArrayList<String>();

		source.values.add("jon");
		source.values.add("arya");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(IndexedOnListField.class),
				source);

		assertThat(indexes.size(), is(2));
		assertThat(indexes,
				IsCollectionContaining.<IndexedData> hasItems(
						new SimpleIndexedPropertyValue(IndexedOnListField.class.getName(), "values", "jon"),
						new SimpleIndexedPropertyValue(IndexedOnListField.class.getName(), "values", "arya")));
	}

	/**
	 * @see DATAREDIS-509
	 */
	@Test
	public void resolveIndexOnPrimitiveArrayField() {

		IndexedOnPrimitiveArrayField source = new IndexedOnPrimitiveArrayField();
		source.values = new int[] { 1, 2, 3 };

		Set<IndexedData> indexes = indexResolver
				.resolveIndexesFor(ClassTypeInformation.from(IndexedOnPrimitiveArrayField.class), source);

		assertThat(indexes.size(), is(3));
		assertThat(indexes,
				IsCollectionContaining.<IndexedData> hasItems(
						new SimpleIndexedPropertyValue(IndexedOnPrimitiveArrayField.class.getName(), "values", 1),
						new SimpleIndexedPropertyValue(IndexedOnPrimitiveArrayField.class.getName(), "values", 2),
						new SimpleIndexedPropertyValue(IndexedOnPrimitiveArrayField.class.getName(), "values", 3)));
	}

	private IndexedData resolve(String path, Object value) {

		Set<IndexedData> data = indexResolver.resolveIndex(KEYSPACE_PERSON, path, propertyMock, value);

		if (data.isEmpty()) {
			return null;
		}

		assertThat(data.size(), is(1));
		return data.iterator().next();
	}

	private Indexed createIndexedInstance() {

		return new Indexed() {

			@Override
			public Class<? extends Annotation> annotationType() {
				return Indexed.class;
			}

		};
	}

	static class IndexedOnListField {

		@Indexed List<String> values;
	}

	static class IndexedOnPrimitiveArrayField {

		@Indexed int[] values;
	}

	static class IndexedOnMapField {

		@Indexed Map<String, String> values;
	}

}
