/*
 * Copyright 2016-2020 the original author or authors.
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
package org.springframework.data.redis.connection.lettuce;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.data.Offset.offset;
import static org.springframework.data.redis.connection.RedisGeoCommands.DistanceUnit.*;
import static org.springframework.data.redis.connection.RedisGeoCommands.GeoRadiusCommandArgs.*;

import reactor.test.StepVerifier;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Metrics;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands.GeoLocation;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveGeoCommandsTests extends LettuceReactiveCommandsTestsBase {

	private static final String ARIGENTO_MEMBER_NAME = "arigento";
	private static final String CATANIA_MEMBER_NAME = "catania";
	private static final String PALERMO_MEMBER_NAME = "palermo";

	private static final Point POINT_ARIGENTO = new Point(13.583333, 37.316667);
	private static final Point POINT_CATANIA = new Point(15.087269, 37.502669);
	private static final Point POINT_PALERMO = new Point(13.361389, 38.115556);

	private static final GeoLocation<ByteBuffer> ARIGENTO = new GeoLocation<>(
			ByteBuffer.wrap(ARIGENTO_MEMBER_NAME.getBytes(Charset.forName("UTF-8"))), POINT_ARIGENTO);
	private static final GeoLocation<ByteBuffer> CATANIA = new GeoLocation<>(
			ByteBuffer.wrap(CATANIA_MEMBER_NAME.getBytes(Charset.forName("UTF-8"))), POINT_CATANIA);
	private static final GeoLocation<ByteBuffer> PALERMO = new GeoLocation<>(
			ByteBuffer.wrap(PALERMO_MEMBER_NAME.getBytes(Charset.forName("UTF-8"))), POINT_PALERMO);

	@Test // DATAREDIS-525
	public void geoAddShouldAddSingleGeoLocationCorrectly() {
		assertThat(connection.geoCommands().geoAdd(KEY_1_BBUFFER, ARIGENTO).block()).isEqualTo(1L);
	}

	@Test // DATAREDIS-525
	public void geoAddShouldAddMultipleGeoLocationsCorrectly() {
		assertThat(connection.geoCommands().geoAdd(KEY_1_BBUFFER, Arrays.asList(ARIGENTO, CATANIA, PALERMO)).block())
				.isEqualTo(3L);
	}

	@Test // DATAREDIS-525
	public void geoDistShouldReturnDistanceInMetersByDefault() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);

		assertThat(connection.geoCommands().geoDist(KEY_1_BBUFFER, PALERMO.getName(), CATANIA.getName()).block().getValue())
				.isCloseTo(166274.15156960033D, offset(0.005));
	}

	@Test // DATAREDIS-525
	public void geoDistShouldReturnDistanceInDesiredMetric() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);

		assertThat(connection.geoCommands().geoDist(KEY_1_BBUFFER, PALERMO.getName(), CATANIA.getName(), Metrics.KILOMETERS)
				.block().getValue()).isCloseTo(166.27415156960033D, offset(0.005));
	}

	@Test // DATAREDIS-525
	public void geoHash() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);

		assertThat(
				connection.geoCommands().geoHash(KEY_1_BBUFFER, Arrays.asList(PALERMO.getName(), CATANIA.getName())).block())
						.containsExactly("sqc8b49rny0", "sqdtr74hyu0");
	}

	@Test // DATAREDIS-525
	public void geoHashNotExisting() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);

		assertThat(connection.geoCommands()
				.geoHash(KEY_1_BBUFFER, Arrays.asList(PALERMO.getName(), ARIGENTO.getName(), CATANIA.getName())).block())
						.containsExactly("sqc8b49rny0", null, "sqdtr74hyu0");
	}

	@Test // DATAREDIS-525
	public void geoPos() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);

		List<Point> result = connection.geoCommands()
				.geoPos(KEY_1_BBUFFER, Arrays.asList(PALERMO.getName(), CATANIA.getName())).block();
		assertThat(result.get(0).getX()).isCloseTo(POINT_PALERMO.getX(), offset(0.005));
		assertThat(result.get(0).getY()).isCloseTo(POINT_PALERMO.getY(), offset(0.005));

		assertThat(result.get(1).getX()).isCloseTo(POINT_CATANIA.getX(), offset(0.005));
		assertThat(result.get(1).getY()).isCloseTo(POINT_CATANIA.getY(), offset(0.005));
	}

	@Test // DATAREDIS-525
	public void geoPosNonExisting() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);

		List<Point> result = connection.geoCommands()
				.geoPos(KEY_1_BBUFFER, Arrays.asList(PALERMO.getName(), ARIGENTO.getName(), CATANIA.getName())).block();
		assertThat(result.get(0).getX()).isCloseTo(POINT_PALERMO.getX(), offset(0.005));
		assertThat(result.get(0).getY()).isCloseTo(POINT_PALERMO.getY(), offset(0.005));

		assertThat(result.get(1)).isNull();

		assertThat(result.get(2).getX()).isCloseTo(POINT_CATANIA.getX(), offset(0.005));
		assertThat(result.get(2).getY()).isCloseTo(POINT_CATANIA.getY(), offset(0.005));
	}

	@Test // DATAREDIS-525
	public void geoRadiusShouldReturnMembersCorrectly() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO_MEMBER_NAME);

		connection.geoCommands().geoRadius(KEY_1_BBUFFER, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)))
				.as(StepVerifier::create) //
				.expectNextCount(3) //
				.expectComplete();

		connection.geoCommands().geoRadius(KEY_1_BBUFFER, new Circle(new Point(15D, 37D), new Distance(150D, KILOMETERS)))
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.expectComplete();
	}

	@Test // DATAREDIS-525
	public void geoRadiusShouldReturnDistanceCorrectly() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO_MEMBER_NAME);

		connection.geoCommands()
				.geoRadius(KEY_1_BBUFFER, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)),
						newGeoRadiusArgs().includeDistance())
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getDistance().getValue()).isCloseTo(130.423D, offset(0.005));
					assertThat(actual.getDistance().getUnit()).isEqualTo("km");
				}) //
				.expectComplete();
	}

	@Test // DATAREDIS-525
	public void geoRadiusShouldApplyLimit() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO_MEMBER_NAME);

		connection.geoCommands()
				.geoRadius(KEY_1_BBUFFER, new Circle(new Point(15D, 37D), new Distance(200D, KILOMETERS)),
						newGeoRadiusArgs().limit(2))
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.expectComplete();
	}

	@Test // DATAREDIS-525
	public void geoRadiusByMemberShouldReturnMembersCorrectly() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO_MEMBER_NAME);

		connection.geoCommands().geoRadiusByMember(KEY_1_BBUFFER, ARIGENTO.getName(), new Distance(100, KILOMETERS))
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(ARIGENTO.getName());
				}) //
				.consumeNextWith(actual -> {
					assertThat(actual.getContent().getName()).isEqualTo(PALERMO.getName());
				}) //
				.expectComplete();
	}

	@Test // DATAREDIS-525
	public void geoRadiusByMemberShouldReturnDistanceCorrectly() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO_MEMBER_NAME);

		connection.geoCommands()
				.geoRadiusByMember(KEY_1_BBUFFER, PALERMO.getName(), new Distance(100, KILOMETERS),
						newGeoRadiusArgs().includeDistance())
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getDistance().getValue()).isCloseTo(90.978D, offset(0.005));
					assertThat(actual.getDistance().getUnit()).isEqualTo("km");
				}) //
				.expectNextCount(1) //
				.verifyComplete();

	}

	@Test // DATAREDIS-525
	public void geoRadiusByMemberShouldApplyLimit() {

		nativeCommands.geoadd(KEY_1, PALERMO.getPoint().getX(), PALERMO.getPoint().getY(), PALERMO_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, CATANIA.getPoint().getX(), CATANIA.getPoint().getY(), CATANIA_MEMBER_NAME);
		nativeCommands.geoadd(KEY_1, ARIGENTO.getPoint().getX(), ARIGENTO.getPoint().getY(), ARIGENTO_MEMBER_NAME);

		connection.geoCommands()
				.geoRadiusByMember(KEY_1_BBUFFER, PALERMO.getName(), new Distance(200, KILOMETERS), newGeoRadiusArgs().limit(2))
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();
	}

}
