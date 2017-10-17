/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.hamcrest.core.IsNull.*;
import static org.hamcrest.number.IsCloseTo.*;
import static org.junit.Assert.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration test of {@link org.springframework.data.redis.core.DefaultGeoOperations}
 *
 * @author Ninad Divadkar
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@RunWith(Parameterized.class)
@IfProfileValue(name = "redisVersion", value = "3.2.0+")
public class DefaultGeoOperationsTests<K, M> {

	public static @ClassRule MinimumRedisVersionRule versionRule = new MinimumRedisVersionRule();

	private static final Point POINT_ARIGENTO = new Point(13.583333, 37.316667);
	private static final Point POINT_CATANIA = new Point(15.087269, 37.502669);
	private static final Point POINT_PALERMO = new Point(13.361389, 38.115556);

	private static final double DISTANCE_PALERMO_CATANIA_METERS = 166274.15156960033;
	private static final double DISTANCE_PALERMO_CATANIA_KILOMETERS = 166.27415156960033;
	private static final double DISTANCE_PALERMO_CATANIA_MILES = 103.31822459492733;
	private static final double DISTANCE_PALERMO_CATANIA_FEET = 545518.8699790037;

	private RedisTemplate<K, M> redisTemplate;
	private ObjectFactory<K> keyFactory;
	private ObjectFactory<M> valueFactory;
	private GeoOperations<K, M> geoOperations;

	public DefaultGeoOperationsTests(RedisTemplate<K, M> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<M> valueFactory) {

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;

		ConnectionFactoryTracker.add(redisTemplate.getConnectionFactory());
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	@Before
	public void setUp() {
		geoOperations = redisTemplate.opsForGeo();
	}

	@After
	public void tearDown() {

		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void testGeoAdd() {

		Long numAdded = geoOperations.add(keyFactory.instance(), POINT_PALERMO, valueFactory.instance());

		assertThat(numAdded, is(1L));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void testGeoAddWithLocationMap() {

		Map<M, Point> memberCoordinateMap = new HashMap<>();
		memberCoordinateMap.put(valueFactory.instance(), POINT_PALERMO);
		memberCoordinateMap.put(valueFactory.instance(), POINT_CATANIA);

		Long numAdded = geoOperations.add(keyFactory.instance(), memberCoordinateMap);

		assertThat(numAdded, is(2L));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoDistShouldReturnDistanceInMetersByDefault() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);

		Distance dist = geoOperations.distance(key, member1, member2);
		assertThat(dist.getValue(), closeTo(DISTANCE_PALERMO_CATANIA_METERS, 0.005));
		assertThat(dist.getUnit(), is(equalTo("m")));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoDistShouldReturnDistanceInKilometersCorrectly() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);

		Distance dist = geoOperations.distance(key, member1, member2, KILOMETERS);
		assertThat(dist.getValue(), closeTo(DISTANCE_PALERMO_CATANIA_KILOMETERS, 0.005));
		assertThat(dist.getUnit(), is(equalTo("km")));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoDistShouldReturnDistanceInMilesCorrectly() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);

		Distance dist = geoOperations.distance(key, member1, member2, DistanceUnit.MILES);
		assertThat(dist.getValue(), closeTo(DISTANCE_PALERMO_CATANIA_MILES, 0.005));
		assertThat(dist.getUnit(), is(equalTo("mi")));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoDistShouldReturnDistanceInFeeCorrectly() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);

		Distance dist = geoOperations.distance(key, member1, member2, DistanceUnit.FEET);
		assertThat(dist.getValue(), closeTo(DISTANCE_PALERMO_CATANIA_FEET, 0.005));
		assertThat(dist.getUnit(), is(equalTo("ft")));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void testGeoHash() {

		K key = keyFactory.instance();
		M v1 = valueFactory.instance();
		M v2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1);
		geoOperations.add(key, POINT_CATANIA, v2);

		List<String> result = geoOperations.hash(key, v1, v2);
		assertThat(result, hasSize(2));

		assertThat(result.get(0), is(equalTo("sqc8b49rny0")));
		assertThat(result.get(1), is(equalTo("sqdtr74hyu0")));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void testGeoPos() {

		K key = keyFactory.instance();
		M v1 = valueFactory.instance();
		M v2 = valueFactory.instance();
		M v3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1);
		geoOperations.add(key, POINT_CATANIA, v2);

		List<Point> result = geoOperations.position(key, v1, v2, v3);// v3 is nonexisting
		assertThat(result, hasSize(3));

		assertThat(result.get(0).getX(), is(closeTo(POINT_PALERMO.getX(), 0.005)));
		assertThat(result.get(0).getY(), is(closeTo(POINT_PALERMO.getY(), 0.005)));

		assertThat(result.get(1).getX(), is(closeTo(POINT_CATANIA.getX(), 0.005)));
		assertThat(result.get(1).getY(), is(closeTo(POINT_CATANIA.getY(), 0.005)));

		assertThat(result.get(2), is(nullValue()));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadiusShouldReturnMembersCorrectly() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);

		GeoResults<GeoLocation<M>> result = geoOperations.radius(key,
				new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)));

		assertThat(result.getContent(), hasSize(2));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadiusShouldReturnLocationsWithDistance() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);

		GeoResults<GeoLocation<M>> result = geoOperations.radius(key,
				new Circle(new Point(15, 37), new Distance(200, KILOMETERS)),
				newGeoRadiusArgs().includeDistance().sortDescending());

		assertThat(result.getContent(), hasSize(2));
		assertThat(result.getContent().get(0).getDistance().getValue(), is(closeTo(190.4424d, 0.005)));
		assertThat(result.getContent().get(0).getDistance().getUnit(), is(equalTo("km")));
		assertThat(result.getContent().get(0).getContent().getName(), is(member1));

		assertThat(result.getContent().get(1).getDistance().getValue(), is(closeTo(56.4413d, 0.005)));
		assertThat(result.getContent().get(1).getDistance().getUnit(), is(equalTo("km")));
		assertThat(result.getContent().get(1).getContent().getName(), is(member2));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadiusShouldReturnLocationsWithCoordinates() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);

		GeoResults<GeoLocation<M>> result = geoOperations.radius(key,
				new Circle(new Point(15, 37), new Distance(200, KILOMETERS)),
				newGeoRadiusArgs().includeCoordinates().sortAscending());

		assertThat(result.getContent(), hasSize(2));
		assertThat(result.getContent().get(0).getContent().getPoint().getX(), is(closeTo(POINT_CATANIA.getX(), 0.005)));
		assertThat(result.getContent().get(0).getContent().getPoint().getY(), is(closeTo(POINT_CATANIA.getY(), 0.005)));
		assertThat(result.getContent().get(0).getContent().getName(), is(member2));

		assertThat(result.getContent().get(1).getContent().getPoint().getX(), is(closeTo(POINT_PALERMO.getX(), 0.005)));
		assertThat(result.getContent().get(1).getContent().getPoint().getY(), is(closeTo(POINT_PALERMO.getY(), 0.005)));
		assertThat(result.getContent().get(1).getContent().getName(), is(member1));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadiusShouldReturnLocationsWithCoordinatesAndDistance() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);

		GeoResults<GeoLocation<M>> result = geoOperations.radius(key,
				new Circle(new Point(15, 37), new Distance(200, KILOMETERS)),
				newGeoRadiusArgs().includeCoordinates().includeDistance().sortAscending());
		assertThat(result.getContent(), hasSize(2));

		assertThat(result.getContent().get(0).getDistance().getValue(), is(closeTo(56.4413d, 0.005)));
		assertThat(result.getContent().get(0).getDistance().getUnit(), is(equalTo("km")));
		assertThat(result.getContent().get(0).getContent().getPoint().getX(), is(closeTo(POINT_CATANIA.getX(), 0.005)));
		assertThat(result.getContent().get(0).getContent().getPoint().getY(), is(closeTo(POINT_CATANIA.getY(), 0.005)));
		assertThat(result.getContent().get(0).getContent().getName(), is(member2));

		assertThat(result.getContent().get(1).getDistance().getValue(), is(closeTo(190.4424d, 0.005)));
		assertThat(result.getContent().get(1).getDistance().getUnit(), is(equalTo("km")));
		assertThat(result.getContent().get(1).getContent().getPoint().getX(), is(closeTo(POINT_PALERMO.getX(), 0.005)));
		assertThat(result.getContent().get(1).getContent().getPoint().getY(), is(closeTo(POINT_PALERMO.getY(), 0.005)));
		assertThat(result.getContent().get(1).getContent().getName(), is(member1));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadiusByMemberShouldReturnMembersCorrectly() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();
		M member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);
		geoOperations.add(key, POINT_ARIGENTO, member3);

		GeoResults<GeoLocation<M>> result = geoOperations.radius(key, member3, new Distance(200, KILOMETERS));
		assertThat(result.getContent(), hasSize(3));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadiusByMemberShouldReturnDistanceCorrectly() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();
		M member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);
		geoOperations.add(key, POINT_ARIGENTO, member3);

		GeoResults<GeoLocation<M>> result = geoOperations.radius(key, member3, new Distance(100, KILOMETERS),
				newGeoRadiusArgs().includeDistance().sortDescending());

		assertThat(result.getContent(), hasSize(2));
		assertThat(result.getContent().get(0).getDistance().getValue(), is(closeTo(90.9778d, 0.005)));
		assertThat(result.getContent().get(0).getContent().getName(), is(member1));
		assertThat(result.getContent().get(1).getDistance().getValue(), is(closeTo(0.0d, 0.005))); // itself
		assertThat(result.getContent().get(1).getContent().getName(), is(member3));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadiusByMemberShouldReturnCoordinates() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();
		M member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);
		geoOperations.add(key, POINT_ARIGENTO, member3);

		GeoResults<GeoLocation<M>> result = geoOperations.radius(key, member3, new Distance(100, DistanceUnit.KILOMETERS),
				newGeoRadiusArgs().includeCoordinates().sortAscending());

		assertThat(result.getContent(), hasSize(2));
		assertThat(result.getContent().get(0).getContent().getPoint().getX(), is(closeTo(POINT_ARIGENTO.getX(), 0.005)));
		assertThat(result.getContent().get(0).getContent().getPoint().getY(), is(closeTo(POINT_ARIGENTO.getY(), 0.005)));
		assertThat(result.getContent().get(0).getContent().getName(), is(member3));

		assertThat(result.getContent().get(1).getContent().getPoint().getX(), is(closeTo(POINT_PALERMO.getX(), 0.005)));
		assertThat(result.getContent().get(1).getContent().getPoint().getY(), is(closeTo(POINT_PALERMO.getY(), 0.005)));
		assertThat(result.getContent().get(1).getContent().getName(), is(member1));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadiusByMemberShouldReturnCoordinatesAndDistance() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();
		M member2 = valueFactory.instance();
		M member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);
		geoOperations.add(key, POINT_CATANIA, member2);
		geoOperations.add(key, POINT_ARIGENTO, member3);

		// with coord and dist, ascending
		GeoResults<GeoLocation<M>> result = geoOperations.radius(key, member1, new Distance(100, KILOMETERS),
				newGeoRadiusArgs().includeCoordinates().includeDistance().sortAscending());
		assertThat(result.getContent(), hasSize(2));

		assertThat(result.getContent().get(0).getDistance().getValue(), is(closeTo(0.0d, 0.005)));
		assertThat(result.getContent().get(0).getContent().getPoint().getX(), is(closeTo(POINT_PALERMO.getX(), 0.005)));
		assertThat(result.getContent().get(0).getContent().getPoint().getY(), is(closeTo(POINT_PALERMO.getY(), 0.005)));
		assertThat(result.getContent().get(0).getContent().getName(), is(member1));

		assertThat(result.getContent().get(1).getDistance().getValue(), is(closeTo(90.9778d, 0.005)));
		assertThat(result.getContent().get(1).getContent().getPoint().getX(), is(closeTo(POINT_ARIGENTO.getX(), 0.005)));
		assertThat(result.getContent().get(1).getContent().getPoint().getY(), is(closeTo(POINT_ARIGENTO.getY(), 0.005)));
		assertThat(result.getContent().get(1).getContent().getName(), is(member3));
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void testGeoRemove() {

		K key = keyFactory.instance();
		M member1 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1);

		assertThat(geoOperations.remove(key, member1), is(1L));
	}
}
