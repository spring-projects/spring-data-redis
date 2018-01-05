/*
 * Copyright 2017-2018 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.ConnectionFactoryTracker;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.test.util.MinimumRedisVersionRule;
import org.springframework.test.annotation.IfProfileValue;

/**
 * Integration tests for {@link DefaultReactiveGeoOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@RunWith(Parameterized.class)
@IfProfileValue(name = "redisVersion", value = "3.2.0+")
public class DefaultReactiveGeoOperationsIntegrationTests<K, V> {

	public static @ClassRule MinimumRedisVersionRule versionRule = new MinimumRedisVersionRule();

	private static final Point POINT_ARIGENTO = new Point(13.583333, 37.316667);
	private static final Point POINT_CATANIA = new Point(15.087269, 37.502669);
	private static final Point POINT_PALERMO = new Point(13.361389, 38.115556);

	private static final double DISTANCE_PALERMO_CATANIA_METERS = 166274.15156960033;
	private static final double DISTANCE_PALERMO_CATANIA_KILOMETERS = 166.27415156960033;

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveGeoOperations<K, V> geoOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

	@Parameters(name = "{4}")
	public static Collection<Object[]> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	@AfterClass
	public static void cleanUp() {
		ConnectionFactoryTracker.cleanUp();
	}

	/**
	 * @param redisTemplate
	 * @param keyFactory
	 * @param valueFactory
	 * @param label parameterized test label, no further use besides that.
	 */
	public DefaultReactiveGeoOperationsIntegrationTests(ReactiveRedisTemplate<K, V> redisTemplate,
			ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, RedisSerializer serializer, String label) {

		this.redisTemplate = redisTemplate;
		this.geoOperations = redisTemplate.opsForGeo();
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;

		ConnectionFactoryTracker.add(redisTemplate.getConnectionFactory());
	}

	@Before
	public void before() {

		RedisConnectionFactory connectionFactory = (RedisConnectionFactory) redisTemplate.getConnectionFactory();
		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoAdd() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(geoOperations.add(key, POINT_PALERMO, value)).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoAddLocation() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		StepVerifier.create(geoOperations.add(key, new GeoLocation<>(value, POINT_PALERMO))) //
				.expectNext(1L) //
				.expectComplete() //
				.verify();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoAddMapOfLocations() {

		K key = keyFactory.instance();

		Map<V, Point> memberCoordinateMap = new HashMap<>();
		memberCoordinateMap.put(valueFactory.instance(), POINT_PALERMO);
		memberCoordinateMap.put(valueFactory.instance(), POINT_CATANIA);

		StepVerifier.create(geoOperations.add(key, memberCoordinateMap)) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoAddIterableOfLocations() {

		K key = keyFactory.instance();

		List<GeoLocation<V>> geoLocations = Arrays.asList(new GeoLocation<>(valueFactory.instance(), POINT_ARIGENTO),
				new GeoLocation<>(valueFactory.instance(), POINT_PALERMO));

		StepVerifier.create(geoOperations.add(key, geoLocations)) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoAddPublisherOfLocations() {

		K key = keyFactory.instance();

		List<GeoLocation<V>> batch1 = Arrays.asList(new GeoLocation<>(valueFactory.instance(), POINT_ARIGENTO),
				new GeoLocation<>(valueFactory.instance(), POINT_PALERMO));

		List<GeoLocation<V>> batch2 = Arrays.asList(new GeoLocation<>(valueFactory.instance(), POINT_CATANIA));

		Flux<List<GeoLocation<V>>> geoLocations = Flux.just(batch1, batch2);

		StepVerifier.create(geoOperations.add(key, geoLocations)).expectNext(2L) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoDistShouldReturnDistanceInMetersByDefault() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		StepVerifier.create(geoOperations.distance(key, member1, member2)) //
				.consumeNextWith(actual -> {

					assertThat(actual.getValue()).isCloseTo(DISTANCE_PALERMO_CATANIA_METERS, offset(0.005));
					assertThat(actual.getUnit()).isEqualTo("m");
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoDistShouldReturnDistanceInKilometersCorrectly() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		StepVerifier.create(geoOperations.distance(key, member1, member2, Metrics.KILOMETERS)) //
				.consumeNextWith(actual -> {

					assertThat(actual.getValue()).isCloseTo(DISTANCE_PALERMO_CATANIA_KILOMETERS, offset(0.005));
					assertThat(actual.getUnit()).isEqualTo("km");
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoHash() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1).block();

		StepVerifier.create(geoOperations.hash(key, v1)) //
				.expectNext("sqc8b49rny0") //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoHashShouldReturnMultipleElements() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1).block();
		geoOperations.add(key, POINT_CATANIA, v2).block();

		StepVerifier.create(geoOperations.hash(key, v1, v3, v2)) //
				.expectNext(Arrays.asList("sqc8b49rny0", null, "sqdtr74hyu0")) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoPos() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1).block();

		StepVerifier.create(geoOperations.position(key, v1)) //
				.consumeNextWith(actual -> {

					assertThat(actual.getX()).isCloseTo(POINT_PALERMO.getX(), offset(0.005));
					assertThat(actual.getY()).isCloseTo(POINT_PALERMO.getY(), offset(0.005));
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoPosShouldReturnMultipleElements() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1).block();
		geoOperations.add(key, POINT_CATANIA, v2).block();

		StepVerifier.create(geoOperations.position(key, v1, v3, v2)) //
				.consumeNextWith(actual -> {

					assertThat(actual.get(0).getX()).isCloseTo(POINT_PALERMO.getX(), offset(0.005));
					assertThat(actual.get(0).getY()).isCloseTo(POINT_PALERMO.getY(), offset(0.005));

					assertThat(actual.get(1)).isNull();
					assertThat(actual.get(2)).isNotNull();
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadius() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		StepVerifier.create(geoOperations.radius(key, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)))) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoRadiusShouldReturnLocationsWithDistance() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		StepVerifier
				.create(geoOperations.radius(key, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)),
						newGeoRadiusArgs().includeDistance().sortDescending())) //
				.consumeNextWith(actual -> {

					assertThat(actual.getDistance().getValue()).isCloseTo(190.4424d, offset(0.005));
					assertThat(actual.getContent().getName()).isEqualTo(member1);
				}) //
				.consumeNextWith(actual -> {

					assertThat(actual.getDistance().getValue()).isCloseTo(56.4413d, offset(0.005));
					assertThat(actual.getContent().getName()).isEqualTo(member2);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	public void geoRadiusByMemberShouldReturnMembersCorrectly() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		StepVerifier.create(geoOperations.radius(key, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)))) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoRadiusByMemberWithin100_000MetersShouldReturnLocations() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();
		V member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();
		geoOperations.add(key, POINT_ARIGENTO, member3).block();

		StepVerifier.create(geoOperations.radius(key, member3, 100_000)) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(member3);
				}) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(member1);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoRadiusByMemberWithin100KMShouldReturnLocations() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();
		V member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();
		geoOperations.add(key, POINT_ARIGENTO, member3).block();

		StepVerifier.create(geoOperations.radius(key, member3, new Distance(100D, KILOMETERS))) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(member3);
				}) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(member1);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoRadiusByMemberShouldReturnLocationsWithDistance() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();
		V member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();
		geoOperations.add(key, POINT_ARIGENTO, member3).block();

		StepVerifier
				.create(geoOperations.radius(key, member3, new Distance(100D, KILOMETERS),
						newGeoRadiusArgs().includeDistance().sortDescending())) //
				.consumeNextWith(actual -> {

					assertThat(actual.getDistance().getValue()).isCloseTo(90.9778, offset(0.005));
					assertThat(actual.getContent().getName()).isEqualTo(member1);
				}) //
				.consumeNextWith(actual -> {

					assertThat(actual.getDistance().getValue()).isCloseTo(0.0, offset(0.005));
					assertThat(actual.getContent().getName()).isEqualTo(member3);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void geoRemove() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		StepVerifier.create(geoOperations.remove(key, member1)) //
				.expectNext(1L) //
				.verifyComplete();
		StepVerifier.create(geoOperations.position(key, member1)) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	public void delete() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();

		StepVerifier.create(geoOperations.delete(key)) //
				.expectNext(true) //
				.verifyComplete();
		StepVerifier.create(geoOperations.position(key, member1)) //
				.verifyComplete();
	}
}
