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
package org.springframework.data.redis.core.convert;

import static org.hamcrest.collection.IsEmptyCollection.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsNull.*;
import static org.junit.Assert.*;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.geo.Point;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.redis.core.convert.ConversionTestEntities.*;
import org.springframework.data.redis.core.index.GeoIndexed;
import org.springframework.data.redis.core.index.IndexConfiguration;
import org.springframework.data.redis.core.index.Indexed;
import org.springframework.data.redis.core.index.SimpleIndexDefinition;
import org.springframework.data.redis.core.mapping.RedisMappingContext;
import org.springframework.data.util.ClassTypeInformation;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.Silent.class)
public class PathIndexResolverUnitTests {

	public @Rule ExpectedException exception = ExpectedException.none();

	IndexConfiguration indexConfig;
	PathIndexResolver indexResolver;

	@Mock PersistentProperty<?> propertyMock;

	@Before
	public void setUp() {

		indexConfig = new IndexConfiguration();
		this.indexResolver = new PathIndexResolver(
				new RedisMappingContext(new MappingConfiguration(indexConfig, new KeyspaceConfiguration())));
	}

	@Test(expected = IllegalArgumentException.class) // DATAREDIS-425
	public void shouldThrowExceptionOnNullMappingContext() {
		new PathIndexResolver(null);
	}

	@Test // DATAREDIS-425
	public void shouldResolveAnnotatedIndexOnRootWhenValueIsNotNull() {

		Address address = new Address();
		address.country = "andor";

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Address.class), address);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(Address.class.getName(), "country", "andor")));
	}

	@Test // DATAREDIS-425
	public void shouldNotResolveAnnotatedIndexOnRootWhenValueIsNull() {

		Address address = new Address();
		address.country = null;

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Address.class), address);

		assertThat(indexes.size(), is(0));
	}

	@Test // DATAREDIS-425
	public void shouldResolveAnnotatedIndexOnNestedObjectWhenValueIsNotNull() {

		Person person = new Person();
		person.address = new Address();
		person.address.country = "andor";

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Person.class), person);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "address.country", "andor")));
	}

	@Test // DATAREDIS-425
	public void shouldResolveMultipleAnnotatedIndexesInLists() {

		TheWheelOfTime twot = new TheWheelOfTime();
		twot.mainCharacters = new ArrayList<>();

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

	@Test // DATAREDIS-425
	public void shouldResolveAnnotatedIndexesInMap() {

		TheWheelOfTime twot = new TheWheelOfTime();
		twot.places = new LinkedHashMap<>();

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

	@Test // DATAREDIS-425
	public void shouldResolveConfiguredIndexesInMapOfSimpleTypes() {

		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "physicalAttributes.eye-color"));

		Person rand = new Person();
		rand.physicalAttributes = new LinkedHashMap<>();
		rand.physicalAttributes.put("eye-color", "grey");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Person.class), rand);

		assertThat(indexes.size(), is(1));
		assertThat(indexes,
				hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "physicalAttributes.eye-color", "grey")));
	}

	@Test // DATAREDIS-425
	public void shouldResolveConfiguredIndexesInMapOfComplexTypes() {

		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "relatives.father.firstname"));

		Person rand = new Person();
		rand.relatives = new LinkedHashMap<>();

		Person janduin = new Person();
		janduin.firstname = "janduin";

		rand.relatives.put("father", janduin);

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Person.class), rand);

		assertThat(indexes.size(), is(1));
		assertThat(indexes,
				hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "relatives.father.firstname", "janduin")));
	}

	@Test // DATAREDIS-425, DATAREDIS-471
	public void shouldIgnoreConfiguredIndexesInMapWhenValueIsNull() {

		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "physicalAttributes.eye-color"));

		Person rand = new Person();
		rand.physicalAttributes = new LinkedHashMap<>();
		rand.physicalAttributes.put("eye-color", null);

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Person.class), rand);

		assertThat(indexes.size(), is(1));
		assertThat(indexes.iterator().next(), IsInstanceOf.instanceOf(RemoveIndexedData.class));
	}

	@Test // DATAREDIS-425
	public void shouldNotResolveIndexOnReferencedEntity() {

		PersonWithAddressReference rand = new PersonWithAddressReference();
		rand.addressRef = new AddressWithId();
		rand.addressRef.id = "emond_s_field";
		rand.addressRef.country = "andor";

		Set<IndexedData> indexes = indexResolver
				.resolveIndexesFor(ClassTypeInformation.from(PersonWithAddressReference.class), rand);

		assertThat(indexes.size(), is(0));
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldReturnNullWhenNoIndexConfigured() {

		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(false);
		assertThat(resolve("foo", "rand"), nullValue());
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldReturnDataWhenIndexConfigured() {

		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(false);
		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "foo"));

		assertThat(resolve("foo", "rand"), notNullValue());
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldReturnDataWhenNoIndexConfiguredButPropertyAnnotated() {

		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(Indexed.class))).thenReturn(createIndexedInstance());

		assertThat(resolve("foo", "rand"), notNullValue());
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldRemovePositionIndicatorForValuesInLists() {

		when(propertyMock.isCollectionLike()).thenReturn(true);
		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(Indexed.class))).thenReturn(createIndexedInstance());

		IndexedData index = resolve("list.[0].name", "rand");

		assertThat(index.getIndexName(), is("list.name"));
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldRemoveKeyIndicatorForValuesInMap() {

		when(propertyMock.isMap()).thenReturn(true);
		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(Indexed.class))).thenReturn(createIndexedInstance());

		IndexedData index = resolve("map.[foo].name", "rand");

		assertThat(index.getIndexName(), is("map.foo.name"));
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldKeepNumericalKeyForValuesInMap() {

		when(propertyMock.isMap()).thenReturn(true);
		when(propertyMock.isAnnotationPresent(eq(Indexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(Indexed.class))).thenReturn(createIndexedInstance());

		IndexedData index = resolve("map.[0].name", "rand");

		assertThat(index.getIndexName(), is("map.0.name"));
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldInspectObjectTypeProperties() {

		Item hat = new Item();
		hat.type = "hat";

		TaVeren mat = new TaVeren();
		mat.feature = hat;

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "feature.type", "hat")));
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldInspectObjectTypePropertiesButIgnoreNullValues() {

		Item hat = new Item();
		hat.description = "wide brimmed hat";

		TaVeren mat = new TaVeren();
		mat.feature = hat;

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(0));
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldInspectObjectTypeValuesInMapProperties() {

		Item hat = new Item();
		hat.type = "hat";

		TaVeren mat = new TaVeren();
		mat.characteristics = new LinkedHashMap<>(2);
		mat.characteristics.put("clothing", hat);
		mat.characteristics.put("gambling", "owns the dark one's luck");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(1));
		assertThat(indexes,
				hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "characteristics.clothing.type", "hat")));
	}

	@Test // DATAREDIS-425
	public void resolveIndexShouldInspectObjectTypeValuesInListProperties() {

		Item hat = new Item();
		hat.type = "hat";

		TaVeren mat = new TaVeren();
		mat.items = new ArrayList<>(2);
		mat.items.add(hat);
		mat.items.add("foxhead medallion");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "items.type", "hat")));
	}

	@Test // DATAREDIS-425
	public void resolveIndexAllowCustomIndexName() {

		indexConfig.addIndexDefinition(new SimpleIndexDefinition(KEYSPACE_PERSON, "items.type", "itemsType"));

		Item hat = new Item();
		hat.type = "hat";

		TaVeren mat = new TaVeren();
		mat.items = new ArrayList<>(2);
		mat.items.add(hat);
		mat.items.add("foxhead medallion");

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(TaVeren.class), mat);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, hasItem(new SimpleIndexedPropertyValue(KEYSPACE_PERSON, "itemsType", "hat")));
	}

	@Test // DATAREDIS-425
	public void resolveIndexForTypeThatHasNoIndexDefined() {

		Size size = new Size();
		size.height = 10;
		size.length = 20;
		size.width = 30;

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(Size.class), size);
		assertThat(indexes, is(empty()));
	}

	@Test // DATAREDIS-425
	public void resolveIndexOnMapField() {

		IndexedOnMapField source = new IndexedOnMapField();
		source.values = new LinkedHashMap<>();

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

	@Test // DATAREDIS-425
	public void resolveIndexOnListField() {

		IndexedOnListField source = new IndexedOnListField();
		source.values = new ArrayList<>();

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

	@Test // DATAREDIS-509
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

	@Test // DATAREDIS-533
	public void resolveGeoIndexShouldMapNameCorrectly() {

		when(propertyMock.isMap()).thenReturn(true);
		when(propertyMock.isAnnotationPresent(eq(GeoIndexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(GeoIndexed.class))).thenReturn(createGeoIndexedInstance());

		IndexedData index = resolve("location", new Point(1D, 2D));

		assertThat(index.getIndexName(), is("location"));
	}

	@Test // DATAREDIS-533
	public void resolveGeoIndexShouldMapNameForNestedPropertyCorrectly() {

		when(propertyMock.isMap()).thenReturn(true);
		when(propertyMock.isAnnotationPresent(eq(GeoIndexed.class))).thenReturn(true);
		when(propertyMock.findAnnotation(eq(GeoIndexed.class))).thenReturn(createGeoIndexedInstance());

		IndexedData index = resolve("property.location", new Point(1D, 2D));

		assertThat(index.getIndexName(), is("property:location"));
	}

	@Test // DATAREDIS-533
	public void resolveGeoIndexOnPointField() {

		GeoIndexedOnPoint source = new GeoIndexedOnPoint();
		source.location = new Point(1D, 2D);

		Set<IndexedData> indexes = indexResolver.resolveIndexesFor(ClassTypeInformation.from(GeoIndexedOnPoint.class),
				source);

		assertThat(indexes.size(), is(1));
		assertThat(indexes, IsCollectionContaining.<IndexedData> hasItems(
				new GeoIndexedPropertyValue(GeoIndexedOnPoint.class.getName(), "location", source.location)));
	}

	@Test // DATAREDIS-533
	public void resolveGeoIndexOnArrayFieldThrowsError() {

		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("GeoIndexed property needs to be of type Point or GeoLocation");

		GeoIndexedOnArray source = new GeoIndexedOnArray();
		source.location = new double[] { 10D, 20D };

		indexResolver.resolveIndexesFor(ClassTypeInformation.from(GeoIndexedOnArray.class), source);
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

	private GeoIndexed createGeoIndexedInstance() {

		return new GeoIndexed() {
			@Override
			public Class<? extends Annotation> annotationType() {
				return GeoIndexed.class;
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

	static class GeoIndexedOnPoint {
		@GeoIndexed Point location;
	}

	static class GeoIndexedOnArray {
		@GeoIndexed double[] location;
	}
}
