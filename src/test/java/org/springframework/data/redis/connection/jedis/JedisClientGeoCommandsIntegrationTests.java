/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchStoreCommandArgs;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.data.redis.test.condition.EnabledOnRedisAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientGeoCommands}. Tests all methods in direct, transaction, and pipelined modes.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisAvailable
@ExtendWith(JedisExtension.class)
class JedisClientGeoCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private JedisClientConnection connection;

	@BeforeEach
	void setUp() {
		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = (JedisClientConnection) factory.getConnection();
	}

	@AfterEach
	void tearDown() {
		if (connection != null) {
			connection.serverCommands().flushDb();
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	// ============ Basic Geo Operations ============
	@Test
	void basicGeoOperationsShouldWork() {
		// Test geoAdd - add single location
		Long addResult = connection.geoCommands().geoAdd("cities".getBytes(), new Point(13.361389, 38.115556),
				"Palermo".getBytes());
		assertThat(addResult).isEqualTo(1L);

		// Test geoAdd - add multiple locations
		Map<byte[], Point> locations = new HashMap<>();
		locations.put("Catania".getBytes(), new Point(15.087269, 37.502669));
		locations.put("Rome".getBytes(), new Point(12.496366, 41.902782));
		Long addMultiResult = connection.geoCommands().geoAdd("cities".getBytes(), locations);
		assertThat(addMultiResult).isEqualTo(2L);

		// Test geoPos - get position
		List<Point> positions = connection.geoCommands().geoPos("cities".getBytes(), "Palermo".getBytes(),
				"Rome".getBytes());
		assertThat(positions).hasSize(2);
		assertThat(positions.get(0)).isNotNull();

		// Test geoDist - get distance between two members
		Distance distance = connection.geoCommands().geoDist("cities".getBytes(), "Palermo".getBytes(),
				"Catania".getBytes());
		assertThat(distance).isNotNull();
		assertThat(distance.getValue()).isGreaterThan(0);

		// Test geoDist with metric
		Distance distanceKm = connection.geoCommands().geoDist("cities".getBytes(), "Palermo".getBytes(),
				"Catania".getBytes(), Metrics.KILOMETERS);
		assertThat(distanceKm).isNotNull();
		assertThat(distanceKm.getValue()).isGreaterThan(0);

		// Test geoHash - get geohash
		List<String> hashes = connection.geoCommands().geoHash("cities".getBytes(), "Palermo".getBytes(),
				"Rome".getBytes());
		assertThat(hashes).hasSize(2);
		assertThat(hashes.get(0)).isNotNull();
	}

	@Test
	void geoRadiusOperationsShouldWork() {
		// Set up test data
		Map<byte[], Point> locations = new HashMap<>();
		locations.put("Palermo".getBytes(), new Point(13.361389, 38.115556));
		locations.put("Catania".getBytes(), new Point(15.087269, 37.502669));
		locations.put("Rome".getBytes(), new Point(12.496366, 41.902782));
		connection.geoCommands().geoAdd("cities".getBytes(), locations);

		// Test geoRadius - find members within radius of point
		Distance radius = new Distance(200, Metrics.KILOMETERS);
		GeoResults<GeoLocation<byte[]>> radiusResult = connection.geoCommands().geoRadius("cities".getBytes(),
				new Circle(new Point(15, 37), radius));
		assertThat(radiusResult.getContent()).isNotEmpty();

		// Test geoRadius with args
		GeoRadiusCommandArgs args = GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().includeCoordinates();
		GeoResults<GeoLocation<byte[]>> radiusWithArgsResult = connection.geoCommands().geoRadius("cities".getBytes(),
				new Circle(new Point(15, 37), radius), args);
		assertThat(radiusWithArgsResult.getContent()).isNotEmpty();

		// Test geoRadiusByMember - find members within radius of member
		GeoResults<GeoLocation<byte[]>> radiusByMemberResult = connection.geoCommands()
				.geoRadiusByMember("cities".getBytes(), "Palermo".getBytes(), radius);
		assertThat(radiusByMemberResult.getContent()).isNotEmpty();

		// Test geoRadiusByMember with args
		GeoResults<GeoLocation<byte[]>> radiusByMemberWithArgsResult = connection.geoCommands()
				.geoRadiusByMember("cities".getBytes(), "Palermo".getBytes(), radius, args);
		assertThat(radiusByMemberWithArgsResult.getContent()).isNotEmpty();
	}

	@Test
	void geoSearchOperationsShouldWork() {
		// Set up test data
		Map<byte[], Point> locations = new HashMap<>();
		locations.put("Palermo".getBytes(), new Point(13.361389, 38.115556));
		locations.put("Catania".getBytes(), new Point(15.087269, 37.502669));
		locations.put("Rome".getBytes(), new Point(12.496366, 41.902782));
		connection.geoCommands().geoAdd("cities".getBytes(), locations);

		// Test geoSearch - search by reference and shape
		GeoReference<byte[]> reference = GeoReference.fromMember("Palermo".getBytes());
		GeoShape shape = GeoShape.byRadius(new Distance(200, Metrics.KILOMETERS));
		GeoSearchCommandArgs args = GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().includeCoordinates();

		GeoResults<GeoLocation<byte[]>> searchResult = connection.geoCommands().geoSearch("cities".getBytes(), reference,
				shape, args);
		assertThat(searchResult.getContent()).isNotEmpty();

		// Test geoSearchStore - search and store results
		GeoSearchStoreCommandArgs storeArgs = GeoSearchStoreCommandArgs.newGeoSearchStoreArgs();
		Long storeResult = connection.geoCommands().geoSearchStore("dest".getBytes(), "cities".getBytes(), reference, shape,
				storeArgs);
		assertThat(storeResult).isGreaterThan(0L);
	}

	@Test
	void geoRemoveOperationShouldWork() {
		// Set up test data
		connection.geoCommands().geoAdd("cities".getBytes(), new Point(13.361389, 38.115556), "Palermo".getBytes());
		connection.geoCommands().geoAdd("cities".getBytes(), new Point(15.087269, 37.502669), "Catania".getBytes());

		// Test geoRemove - remove member
		Long removeResult = connection.geoCommands().geoRemove("cities".getBytes(), "Palermo".getBytes());
		assertThat(removeResult).isEqualTo(1L);

		// Verify removal
		List<Point> positions = connection.geoCommands().geoPos("cities".getBytes(), "Palermo".getBytes());
		assertThat(positions.get(0)).isNull();
	}

	@Test
	void transactionShouldExecuteAtomically() {
		// Set up initial state
		Map<byte[], Point> locations = new HashMap<>();
		locations.put("Palermo".getBytes(), new Point(13.361389, 38.115556));
		locations.put("Catania".getBytes(), new Point(15.087269, 37.502669));
		connection.geoCommands().geoAdd("txCities".getBytes(), locations);

		// Execute multiple geo operations in a transaction
		connection.multi();
		connection.geoCommands().geoAdd("txCities".getBytes(), new Point(12.496366, 41.902782), "Rome".getBytes());
		connection.geoCommands().geoPos("txCities".getBytes(), "Palermo".getBytes());
		connection.geoCommands().geoDist("txCities".getBytes(), "Palermo".getBytes(), "Catania".getBytes());
		connection.geoCommands().geoHash("txCities".getBytes(), "Palermo".getBytes());
		List<Object> results = connection.exec();

		// Verify all commands executed
		assertThat(results).hasSize(4);
		assertThat(results.get(0)).isEqualTo(1L); // geoAdd result
		assertThat(results.get(1)).isInstanceOf(List.class); // geoPos result
		assertThat(results.get(2)).isInstanceOf(Distance.class); // geoDist result
		assertThat(results.get(3)).isInstanceOf(List.class); // geoHash result
	}

	@Test
	void pipelineShouldExecuteMultipleCommands() {
		// Set up initial state
		Map<byte[], Point> locations = new HashMap<>();
		locations.put("Palermo".getBytes(), new Point(13.361389, 38.115556));
		locations.put("Catania".getBytes(), new Point(15.087269, 37.502669));
		connection.geoCommands().geoAdd("pipeCities".getBytes(), locations);

		// Execute multiple geo operations in pipeline
		connection.openPipeline();
		connection.geoCommands().geoAdd("pipeCities".getBytes(), new Point(12.496366, 41.902782), "Rome".getBytes());
		connection.geoCommands().geoPos("pipeCities".getBytes(), "Palermo".getBytes(), "Rome".getBytes());
		connection.geoCommands().geoDist("pipeCities".getBytes(), "Palermo".getBytes(), "Catania".getBytes(),
				Metrics.KILOMETERS);
		connection.geoCommands().geoHash("pipeCities".getBytes(), "Palermo".getBytes(), "Catania".getBytes());
		connection.geoCommands().geoRemove("pipeCities".getBytes(), "Rome".getBytes());
		List<Object> results = connection.closePipeline();

		// Verify all command results
		assertThat(results).hasSize(5);
		assertThat(results.get(0)).isEqualTo(1L); // geoAdd result
		@SuppressWarnings("unchecked")
		List<Point> positions = (List<Point>) results.get(1);
		assertThat(positions).hasSize(2); // geoPos result
		assertThat(results.get(2)).isInstanceOf(Distance.class); // geoDist result
		@SuppressWarnings("unchecked")
		List<String> hashes = (List<String>) results.get(3);
		assertThat(hashes).hasSize(2); // geoHash result
		assertThat(results.get(4)).isEqualTo(1L); // geoRemove result
	}
}
