/*
 * Copyright 2013-2014 the original author or authors.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RedisTestProfileValueSource;
import org.springframework.data.redis.TestCondition;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.springframework.data.redis.SpinBarrier.waitFor;
import static org.springframework.data.redis.matcher.RedisTestMatchers.isEqual;

/**
 * Integration test of {@link org.springframework.data.redis.core.DefaultGeoOperations}
 *
 * @author Ninad Divadkar
 */
@RunWith(Parameterized.class)
public class DefaultGeoOperationsTests<K, M> {

	private RedisTemplate<K, M> redisTemplate;

	private ObjectFactory<K> keyFactory;

	private ObjectFactory<M> valueFactory;

	private GeoOperations<K, M> geoOperations;

	public DefaultGeoOperationsTests(RedisTemplate<K, M> redisTemplate, ObjectFactory<K> keyFactory,
                                     ObjectFactory<M> valueFactory) {
		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
	}

	@Parameters
	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@Before
	public void setUp() {
		geoOperations = redisTemplate.opsForGeo();
	}

	@After
	public void tearDown() {
		redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) {
				connection.flushDb();
				return null;
			}
		});
	}

	@Test
	public void testGeoAdd() throws Exception {
		K key = keyFactory.instance();
        M v1 = valueFactory.instance();
		Long numAdded = geoOperations.geoAdd(key, 13.361389, 38.115556, v1);
		assertEquals(numAdded.longValue(), 1L);

//        numAdded = geoOperations.geoAdd("Sicily", 15.087269, 37.502669, "Catania");
//		assertEquals(numAdded.longValue(), 2L);
	}

    @Test
    public void testGeoAdd2() throws Exception {
        K key = keyFactory.instance();
        Map<M, GeoCoordinate> memberCoordinateMap = new HashMap<M, GeoCoordinate>();
        memberCoordinateMap.put(valueFactory.instance(), new GeoCoordinate(2.2323, 43.324));
        memberCoordinateMap.put(valueFactory.instance(), new GeoCoordinate(12.993, 31.3994));
        Long numAdded = geoOperations.geoAdd(key, memberCoordinateMap);
        assertEquals(numAdded.longValue(), 2L);

//        numAdded = geoOperations.geoAdd("Sicily", 15.087269, 37.502669, "Catania");
//		assertEquals(numAdded.longValue(), 2L);
    }

    @Test
    public void testGeoDist() throws Exception {
        K key = keyFactory.instance();
        M v1 = valueFactory.instance();
        M v2 = valueFactory.instance();

        geoOperations.geoAdd(key, 13.361389, 38.115556, v1);
        geoOperations.geoAdd(key, 15.087269, 37.502669, v2);

        Double dist = geoOperations.geoDist(key, v1, v2);
        assertEquals(dist.doubleValue(), 166274.15156960033, 0.00001); // gives in meters

        dist = geoOperations.geoDist(key, v1, v2, GeoUnit.KiloMeters);
        assertEquals(dist.doubleValue(), 166.27415156960033, 0.00001);

        dist = geoOperations.geoDist(key, v1, v2, GeoUnit.Miles);
        assertEquals(dist.doubleValue(), 103.31822459492733, 0.00001);

        dist = geoOperations.geoDist(key, v1, v2, GeoUnit.Feet);
        assertEquals(dist.doubleValue(), 545518.8699790037, 0.00001);
    }

    @Test
    public void testGeoHash() throws Exception {
        K key = keyFactory.instance();
        M v1 = valueFactory.instance();
        M v2 = valueFactory.instance();

        geoOperations.geoAdd(key, 13.361389, 38.115556, v1);
        geoOperations.geoAdd(key, 15.087269, 37.502669, v2);

        List<byte[]> result = geoOperations.geoHash(key, v1, v2);
        assertEquals(result.size(), 2);

        final RedisSerializer<String> serializer = new StringRedisSerializer();

        assertEquals(serializer.deserialize(result.get(0)), "sqc8b49rny0");
        assertEquals(serializer.deserialize(result.get(1)), "sqdtr74hyu0");
    }

    @Test
    public void testGeoPos() throws Exception {
        K key = keyFactory.instance();
        M v1 = valueFactory.instance();
        M v2 = valueFactory.instance();
        M v3 = valueFactory.instance();

        geoOperations.geoAdd(key, 13.361389, 38.115556, v1);
        geoOperations.geoAdd(key, 15.087269, 37.502669, v2);

        List<GeoCoordinate> result = geoOperations.geoPos(key, v1, v2, v3);// v3 is nonexisting
        assertEquals(result.size(), 3);

        assertEquals(result.get(0).getLongitude(), 13.361389338970184, 0.000001);
        assertEquals(result.get(0).getLatitude(), 38.115556395496299, 0.000001);

        assertEquals(result.get(1).getLongitude(), 15.087267458438873, 0.000001);
        assertEquals(result.get(1).getLatitude(), 37.50266842333162, 0.000001);

        assertNull(result.get(2));
    }
}
