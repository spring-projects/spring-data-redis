/*
 * Copyright 2017-2025 the original author or authors.
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
import static org.springframework.data.redis.connection.RedisGeoCommands.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs.*;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveOperationsTestParams.Fixture;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.data.redis.test.condition.EnabledOnCommand;

/**
 * Integration tests for {@link DefaultReactiveGeoOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 */
@ParameterizedClass
@MethodSource("testParams")
@EnabledOnCommand("GEOADD")
public class DefaultReactiveGeoOperationsIntegrationTests<K, V> {

	private static final Point POINT_ARIGENTO = new Point(13.583333, 37.316667);
	private static final Point POINT_CATANIA = new Point(15.087269, 37.502669);
	private static final Point POINT_PALERMO = new Point(13.361389, 38.115556);

	private static final double DISTANCE_PALERMO_CATANIA_METERS = 166274.15156960033;
	private static final double DISTANCE_PALERMO_CATANIA_KILOMETERS = 166.27415156960033;

	private final ReactiveRedisTemplate<K, V> redisTemplate;
	private final ReactiveGeoOperations<K, V> geoOperations;

	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;

	public static Collection<Fixture<?, ?>> testParams() {
		return ReactiveOperationsTestParams.testParams();
	}

	public DefaultReactiveGeoOperationsIntegrationTests(Fixture<K, V> fixture) {

		this.redisTemplate = fixture.getTemplate();
		this.geoOperations = redisTemplate.opsForGeo();
		this.keyFactory = fixture.getKeyFactory();
		this.valueFactory = fixture.getValueFactory();
	}

	@BeforeEach
	void before() {

		RedisConnectionFactory connectionFactory = (RedisConnectionFactory) redisTemplate.getConnectionFactory();
		RedisConnection connection = connectionFactory.getConnection();
		connection.flushAll();
		connection.close();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoAdd() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, value) //
				.as(StepVerifier::create).expectNext(1L).verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoAddLocation() {

		K key = keyFactory.instance();
		V value = valueFactory.instance();

		geoOperations.add(key, new GeoLocation<>(value, POINT_PALERMO)) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.expectComplete() //
				.verify();
	}

	@Test
	// DATAREDIS-602, DATAREDIS-614
	void geoAddMapOfLocations() {

		K key = keyFactory.instance();

		Map<V, Point> memberCoordinateMap = new HashMap<>();
		memberCoordinateMap.put(valueFactory.instance(), POINT_PALERMO);
		memberCoordinateMap.put(valueFactory.instance(), POINT_CATANIA);

		geoOperations.add(key, memberCoordinateMap) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoAddIterableOfLocations() {

		K key = keyFactory.instance();

		List<GeoLocation<V>> geoLocations = Arrays.asList(new GeoLocation<>(valueFactory.instance(), POINT_ARIGENTO),
				new GeoLocation<>(valueFactory.instance(), POINT_PALERMO));

		geoOperations.add(key, geoLocations) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoAddPublisherOfLocations() {

		K key = keyFactory.instance();

		List<GeoLocation<V>> batch1 = Arrays.asList(new GeoLocation<>(valueFactory.instance(), POINT_ARIGENTO),
				new GeoLocation<>(valueFactory.instance(), POINT_PALERMO));

		List<GeoLocation<V>> batch2 = Arrays.asList(new GeoLocation<>(valueFactory.instance(), POINT_CATANIA));

		Flux<List<GeoLocation<V>>> geoLocations = Flux.just(batch1, batch2);

		geoOperations.add(key, geoLocations) //
				.as(StepVerifier::create).expectNext(2L) //
				.expectNext(1L) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoDistShouldReturnDistanceInMetersByDefault() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		geoOperations.distance(key, member1, member2) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getValue()).isCloseTo(DISTANCE_PALERMO_CATANIA_METERS, offset(0.005));
					assertThat(actual.getUnit()).isEqualTo("m");
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoDistShouldReturnDistanceInKilometersCorrectly() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		geoOperations.distance(key, member1, member2, Metrics.KILOMETERS) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getValue()).isCloseTo(DISTANCE_PALERMO_CATANIA_KILOMETERS, offset(0.005));
					assertThat(actual.getUnit()).isEqualTo("km");
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoHash() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1).block();

		geoOperations.hash(key, v1) //
				.as(StepVerifier::create) //
				.expectNext("sqc8b49rny0") //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoHashShouldReturnMultipleElements() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1).block();
		geoOperations.add(key, POINT_CATANIA, v2).block();

		geoOperations.hash(key, v1, v3, v2) //
				.as(StepVerifier::create) //
				.expectNext(Arrays.asList("sqc8b49rny0", null, "sqdtr74hyu0")) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoPos() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1).block();

		geoOperations.position(key, v1) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getX()).isCloseTo(POINT_PALERMO.getX(), offset(0.005));
					assertThat(actual.getY()).isCloseTo(POINT_PALERMO.getY(), offset(0.005));
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoPosShouldReturnMultipleElements() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, v1).block();
		geoOperations.add(key, POINT_CATANIA, v2).block();

		geoOperations.position(key, v1, v3, v2) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.get(0).getX()).isCloseTo(POINT_PALERMO.getX(), offset(0.005));
					assertThat(actual.get(0).getY()).isCloseTo(POINT_PALERMO.getY(), offset(0.005));

					assertThat(actual.get(1)).isNull();
					assertThat(actual.get(2)).isNotNull();
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-438, DATAREDIS-614
	void geoRadius() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		geoOperations.radius(key, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS))).as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoRadiusShouldReturnLocationsWithDistance() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		geoOperations
				.radius(key, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)),
						newGeoRadiusArgs().includeDistance().sortDescending())
				.as(StepVerifier::create) //
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
	void geoRadiusByMemberShouldReturnMembersCorrectly() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		geoOperations.radius(key, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS))).as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoRadiusByMemberWithin100_000MetersShouldReturnLocations() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();
		V member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();
		geoOperations.add(key, POINT_ARIGENTO, member3).block();

		geoOperations.radius(key, member3, 100_000).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(member3);
				}) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(member1);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoRadiusByMemberWithin100KMShouldReturnLocations() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();
		V member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();
		geoOperations.add(key, POINT_ARIGENTO, member3).block();

		geoOperations.radius(key, member3, new Distance(100D, KILOMETERS)).as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(member3);
				}) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(member1);
				}) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void geoRadiusByMemberShouldReturnLocationsWithDistance() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();
		V member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();
		geoOperations.add(key, POINT_ARIGENTO, member3).block();

		geoOperations
				.radius(key, member3, new Distance(100D, KILOMETERS), newGeoRadiusArgs().includeDistance().sortDescending())
				.as(StepVerifier::create) //
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
	void geoRemove() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();

		geoOperations.remove(key, member1) //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();
		geoOperations.position(key, member1) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // DATAREDIS-602, DATAREDIS-614
	void delete() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();

		geoOperations.delete(key) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
		geoOperations.position(key, member1) //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test // GH-2043
	@EnabledOnCommand("GEOSEARCH")
	void geoSearchShouldReturnLocationsWithDistance() {

		K key = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();
		V member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();
		geoOperations.add(key, POINT_ARIGENTO, member3).block();

		geoOperations
				.search(key, GeoReference.fromMember(member3), GeoShape.byRadius(new Distance(100D, KILOMETERS)),
						newGeoRadiusArgs().includeDistance().sortDescending())
				.as(StepVerifier::create) //
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

	@Test // GH-2043
	@EnabledOnCommand("GEOSEARCH")
	void geoSearchAndStoreShouldStoreItems() {

		K key = keyFactory.instance();
		K destKey = keyFactory.instance();
		V member1 = valueFactory.instance();
		V member2 = valueFactory.instance();
		V member3 = valueFactory.instance();

		geoOperations.add(key, POINT_PALERMO, member1).block();
		geoOperations.add(key, POINT_CATANIA, member2).block();
		geoOperations.add(key, POINT_ARIGENTO, member3).block();

		geoOperations.searchAndStore(key, destKey, GeoReference.fromMember(member3), new Distance(100D, KILOMETERS))
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();

		redisTemplate.opsForZSet().size(destKey) //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}
}
