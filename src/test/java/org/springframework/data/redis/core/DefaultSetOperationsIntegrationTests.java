/*
 * Copyright 2013-2025 the original author or authors.
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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.test.condition.EnabledOnCommand;

/**
 * Integration test of {@link DefaultSetOperations}
 *
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 * @author Mingi Lee
 */
@ParameterizedClass
@MethodSource("testParams")
@SuppressWarnings("unchecked")
public class DefaultSetOperationsIntegrationTests<K, V> {

	private final RedisTemplate<K, V> redisTemplate;
	private final ObjectFactory<K> keyFactory;
	private final ObjectFactory<V> valueFactory;
	private final SetOperations<K, V> setOps;

	public DefaultSetOperationsIntegrationTests(RedisTemplate<K, V> redisTemplate, ObjectFactory<K> keyFactory,
			ObjectFactory<V> valueFactory) {

		this.redisTemplate = redisTemplate;
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.setOps = redisTemplate.opsForSet();
	}

	public static Collection<Object[]> testParams() {
		return AbstractOperationsTestParams.testParams();
	}

	@BeforeEach
	void setUp() {
		redisTemplate.execute((RedisCallback<Object>) connection -> {
			connection.flushDb();
			return null;
		});
	}

	@SuppressWarnings("unchecked")
	@Test
	void testDistinctRandomMembers() {

		K setKey = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		setOps.add(setKey, v1);
		setOps.add(setKey, v2);

		Set<V> members = setOps.distinctRandomMembers(setKey, 2);
		assertThat(members).contains(v1, v2);
	}

	@Test
	void testRandomMembersWithDuplicates() {

		K setKey = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		setOps.add(setKey, v1);

		List<V> members = setOps.randomMembers(setKey, 2);
		assertThat(members).hasSize(2).contains(v1);
	}

	@Test
	void testRandomMembersNegative() {

		try {
			setOps.randomMembers(keyFactory.instance(), -1);
			fail("IllegalArgumentException should be thrown");
		} catch (IllegalArgumentException expected) {
		}
	}

	@Test
	void testDistinctRandomMembersNegative() {

		try {
			setOps.distinctRandomMembers(keyFactory.instance(), -2);
			fail("IllegalArgumentException should be thrown");
		} catch (IllegalArgumentException expected) {
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void testMove() {

		K key1 = keyFactory.instance();
		K key2 = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		setOps.add(key1, v1);
		setOps.add(key1, v2);
		setOps.move(key1, v1, key2);

		assertThat(setOps.members(key1)).containsOnly(v2);
		assertThat(setOps.members(key2)).containsOnly(v1);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testPop() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();

		setOps.add(key, v1);

		assertThat(setOps.pop(key)).isEqualTo(v1);
		assertThat(setOps.members(key)).isEmpty();
	}

	@Test // DATAREDIS-668
	void testPopWithCount() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		setOps.add(key, v1, v2, v3);

		List<V> result = setOps.pop(key, 2);
		assertThat(result).hasSize(2);
		assertThat(result.get(0)).isInstanceOf(v1.getClass());
	}

	@Test
	void testRandomMember() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();

		setOps.add(key, v1);

		assertThat(setOps.randomMember(key)).isEqualTo(v1);
	}

	@Test
	void testAdd() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();

		assertThat(setOps.add(key, v1, v2)).isEqualTo(Long.valueOf(2));
		assertThat(setOps.members(key)).containsOnly(v1, v2);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testRemove() {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(key, v1, v2, v3);
		assertThat(setOps.remove(key, v1, v2, v4)).isEqualTo(Long.valueOf(2));

		assertThat(setOps.members(key)).containsOnly(v3);
	}

	@Test // DATAREDIS-304
	@SuppressWarnings("unchecked")
	void testSSCanReadsValuesFully() throws IOException {

		K key = keyFactory.instance();
		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();

		setOps.add(key, v1, v2, v3);
		long count = 0;
		try (Cursor<V> it = setOps.scan(key, ScanOptions.scanOptions().count(1).build())) {
			while (it.hasNext()) {
				assertThat(it.next()).isIn(v1, v2, v3);
				count++;
			}
		}

		assertThat(count).isEqualTo(setOps.size(key));
	}

	@Test // DATAREDIS-873
	void diffShouldReturnDifference() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.difference(Arrays.asList(sourceKey1, sourceKey2))).contains(v1);
	}

	@Test // DATAREDIS-873
	void diffAndStoreShouldReturnDifferenceShouldReturnNumberOfElementsInDestination() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();
		K destinationKey = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.differenceAndStore(Arrays.asList(sourceKey1, sourceKey2), destinationKey)).isEqualTo(1L);
	}

	@Test // DATAREDIS-873
	void unionShouldConcatSets() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.union(Arrays.asList(sourceKey1, sourceKey2))).contains(v1, v2, v3, v4);
	}

	@Test // DATAREDIS-873
	void unionAndStoreShouldReturnDifferenceShouldReturnNumberOfElementsInDestination() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();
		K destinationKey = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.unionAndStore(Arrays.asList(sourceKey1, sourceKey2), destinationKey)).isEqualTo(4L);
	}

	@Test // DATAREDIS-873
	void intersectShouldReturnElements() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.intersect(Arrays.asList(sourceKey1, sourceKey2))).hasSize(2);
	}

	@Test // DATAREDIS-448, DATAREDIS-873
	void intersectAndStoreShouldReturnNumberOfElementsInDestination() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();
		K destinationKey = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);

		assertThat(setOps.intersectAndStore(sourceKey1, sourceKey2, destinationKey)).isEqualTo(2L);
		assertThat(setOps.intersectAndStore(Arrays.asList(sourceKey1, sourceKey2), destinationKey)).isEqualTo(2L);
	}

	@Test // GH-2906
	@EnabledOnCommand("SINTERCARD")
	void intersectSizeShouldReturnIntersectionCardinality() {

		K sourceKey1 = keyFactory.instance();
		K sourceKey2 = keyFactory.instance();
		K sourceKey3 = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();
		V v5 = valueFactory.instance();

		setOps.add(sourceKey1, v1, v2, v3);
		setOps.add(sourceKey2, v2, v3, v4);
		setOps.add(sourceKey3, v3, v4, v5);

		// Test two keys intersection
		assertThat(setOps.intersectSize(sourceKey1, sourceKey2)).isEqualTo(2L);

		// Test key and collection intersection
		assertThat(setOps.intersectSize(sourceKey1, Arrays.asList(sourceKey2, sourceKey3))).isEqualTo(1L);

		// Test collection intersection
		assertThat(setOps.intersectSize(Arrays.asList(sourceKey1, sourceKey2, sourceKey3))).isEqualTo(1L);

		// Test empty intersection
		K emptyKey = keyFactory.instance();
		assertThat(setOps.intersectSize(sourceKey1, emptyKey)).isEqualTo(0L);
	}

	@Test // GH-2037
	@EnabledOnCommand("SMISMEMBER")
	void isMember() {

		K key = keyFactory.instance();

		V v1 = valueFactory.instance();
		V v2 = valueFactory.instance();
		V v3 = valueFactory.instance();
		V v4 = valueFactory.instance();

		setOps.add(key, v1, v2, v3);

		assertThat(setOps.isMember(key, v1)).isTrue();
		assertThat(setOps.isMember(key, v4)).isFalse();
		assertThat(setOps.isMember(key, v1, v2, v3, v4)).containsEntry(v1, true).containsEntry(v2, true)
				.containsEntry(v3, true).containsEntry(v4, false);
	}
}
