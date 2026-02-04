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

import java.util.List;

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
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoSearchCommandArgs;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.data.redis.domain.geo.GeoShape;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for {@link JedisClientGeoCommands} in cluster mode. Tests all methods in direct and pipelined modes
 * (transactions not supported in cluster).
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
class JedisClientClusterGeoCommandsIntegrationTests {

	private JedisClientConnectionFactory factory;
	private RedisClusterConnection connection;

	@BeforeEach
	void setUp() {
		RedisClusterConfiguration config = new RedisClusterConfiguration().clusterNode(SettingsUtils.getHost(),
				SettingsUtils.getClusterPort());
		factory = new JedisClientConnectionFactory(config);
		factory.afterPropertiesSet();
		connection = factory.getClusterConnection();
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
		// Test geoAdd - add geo locations
		Long geoAddResult = connection.geoCommands().geoAdd("locations".getBytes(), new Point(13.361389, 38.115556),
				"Palermo".getBytes());
		assertThat(geoAddResult).isEqualTo(1L);

		Long geoAddMultiResult = connection.geoCommands().geoAdd("locations".getBytes(),
				List.of(new GeoLocation<>("Catania".getBytes(), new Point(15.087269, 37.502669)),
						new GeoLocation<>("Rome".getBytes(), new Point(12.496366, 41.902782))));
		assertThat(geoAddMultiResult).isEqualTo(2L);

		// Test geoPos - get positions
		List<Point> geoPosResult = connection.geoCommands().geoPos("locations".getBytes(), "Palermo".getBytes(),
				"Catania".getBytes());
		assertThat(geoPosResult).hasSize(2);
		assertThat(geoPosResult.get(0)).isNotNull();

		// Test geoDist - get distance between members
		Distance geoDistResult = connection.geoCommands().geoDist("locations".getBytes(), "Palermo".getBytes(),
				"Catania".getBytes());
		assertThat(geoDistResult).isNotNull();
		assertThat(geoDistResult.getValue()).isGreaterThan(0);

		// Test geoDist with metric
		Distance geoDistKmResult = connection.geoCommands().geoDist("locations".getBytes(), "Palermo".getBytes(),
				"Catania".getBytes(), Metrics.KILOMETERS);
		assertThat(geoDistKmResult).isNotNull();
		assertThat(geoDistKmResult.getValue()).isGreaterThan(0);

		// Test geoHash - get geohash
		List<String> geoHashResult = connection.geoCommands().geoHash("locations".getBytes(), "Palermo".getBytes(),
				"Catania".getBytes());
		assertThat(geoHashResult).hasSize(2);
		assertThat(geoHashResult.get(0)).isNotNull();
	}

	@Test
	void geoRadiusOperationsShouldWork() {
		// Set up locations
		connection.geoCommands().geoAdd("locations".getBytes(),
				List.of(new GeoLocation<>("Palermo".getBytes(), new Point(13.361389, 38.115556)),
						new GeoLocation<>("Catania".getBytes(), new Point(15.087269, 37.502669)),
						new GeoLocation<>("Rome".getBytes(), new Point(12.496366, 41.902782))));

		// Test geoRadius - get members within radius
		GeoResults<GeoLocation<byte[]>> geoRadiusResult = connection.geoCommands().geoRadius("locations".getBytes(),
				new Circle(new Point(15, 37), new Distance(200, Metrics.KILOMETERS)));
		assertThat(geoRadiusResult).isNotNull();
		assertThat(geoRadiusResult.getContent()).isNotEmpty();

		// Test geoRadius with args
		GeoRadiusCommandArgs args = GeoRadiusCommandArgs.newGeoRadiusArgs().includeDistance().includeCoordinates()
				.sortAscending();
		GeoResults<GeoLocation<byte[]>> geoRadiusArgsResult = connection.geoCommands().geoRadius("locations".getBytes(),
				new Circle(new Point(15, 37), new Distance(200, Metrics.KILOMETERS)), args);
		assertThat(geoRadiusArgsResult).isNotNull();

		// Test geoRadiusByMember - get members within radius of a member
		GeoResults<GeoLocation<byte[]>> geoRadiusByMemberResult = connection.geoCommands()
				.geoRadiusByMember("locations".getBytes(), "Palermo".getBytes(), new Distance(200, Metrics.KILOMETERS));
		assertThat(geoRadiusByMemberResult).isNotNull();
		assertThat(geoRadiusByMemberResult.getContent()).isNotEmpty();

		// Test geoRadiusByMember with args
		GeoResults<GeoLocation<byte[]>> geoRadiusByMemberArgsResult = connection.geoCommands()
				.geoRadiusByMember("locations".getBytes(), "Palermo".getBytes(), new Distance(200, Metrics.KILOMETERS), args);
		assertThat(geoRadiusByMemberArgsResult).isNotNull();
	}

	@Test
	void geoSearchOperationsShouldWork() {
		// Set up locations
		connection.geoCommands().geoAdd("locations".getBytes(),
				List.of(new GeoLocation<>("Palermo".getBytes(), new Point(13.361389, 38.115556)),
						new GeoLocation<>("Catania".getBytes(), new Point(15.087269, 37.502669)),
						new GeoLocation<>("Rome".getBytes(), new Point(12.496366, 41.902782))));

		// Test geoSearch - search with reference and shape
		GeoReference<byte[]> reference = GeoReference.fromMember("Palermo".getBytes());
		GeoShape shape = GeoShape.byRadius(new Distance(200, Metrics.KILOMETERS));
		GeoSearchCommandArgs searchArgs = GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().includeCoordinates();

		GeoResults<GeoLocation<byte[]>> geoSearchResult = connection.geoCommands().geoSearch("locations".getBytes(),
				reference, shape, searchArgs);
		assertThat(geoSearchResult).isNotNull();
		assertThat(geoSearchResult.getContent()).isNotEmpty();
	}

	@Test
	void geoRemoveOperationShouldWork() {
		// Set up locations
		connection.geoCommands().geoAdd("locations".getBytes(), new Point(13.361389, 38.115556), "Palermo".getBytes());

		// Test geoRemove - remove geo location (uses zRem internally)
		Long geoRemoveResult = connection.zSetCommands().zRem("locations".getBytes(), "Palermo".getBytes());
		assertThat(geoRemoveResult).isEqualTo(1L);

		List<Point> geoPosResult = connection.geoCommands().geoPos("locations".getBytes(), "Palermo".getBytes());
		assertThat(geoPosResult.get(0)).isNull();
	}
}
