/*
 * Copyright 2011-present the original author or authors.
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
package org.springframework.data.redis.support.collections;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assumptions.*;

import java.io.IOException;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.MethodSource;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.DoubleAsStringObjectFactory;
import org.springframework.data.redis.LongAsStringObjectFactory;
import org.springframework.data.redis.ObjectFactory;
import org.springframework.data.redis.RawObjectFactory;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.BoundHashFieldExpirationOperations;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ExpireChanges;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.test.condition.EnabledOnCommand;

/**
 * Integration test for Redis Map.
 *
 * @author Costin Leau
 * @author Jennifer Hickey
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Christian Bühler
 */
@MethodSource("testParams")
public abstract class AbstractRedisMapIntegrationTests<K, V> {

	protected RedisMap<K, V> map;
	protected ObjectFactory<K> keyFactory;
	protected ObjectFactory<V> valueFactory;
	@SuppressWarnings("rawtypes") protected RedisTemplate template;

	@SuppressWarnings("rawtypes")
	AbstractRedisMapIntegrationTests(ObjectFactory<K> keyFactory, ObjectFactory<V> valueFactory, RedisTemplate template) {
		this.keyFactory = keyFactory;
		this.valueFactory = valueFactory;
		this.template = template;
	}

	abstract RedisMap<K, V> createMap();

	@BeforeEach
	void setUp() {
		template.execute((RedisCallback<Object>) connection -> {
			connection.flushAll();
			return null;
		});
		map = createMap();
	}

	protected K getKey() {
		return keyFactory.instance();
	}

	protected V getValue() {
		return valueFactory.instance();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected RedisStore copyStore(RedisStore store) {
		return new DefaultRedisMap(store.getKey(), store.getOperations());
	}

	@Test
	void testClear() {
		map.clear();
		assertThat(map.size()).isEqualTo(0);
		map.put(getKey(), getValue());
		assertThat(map.size()).isEqualTo(1);
		map.clear();
		assertThat(map.size()).isEqualTo(0);
	}

	@Test
	void testContainsKey() {
		K k1 = getKey();
		K k2 = getKey();

		assertThat(map.containsKey(k1)).isFalse();
		assertThat(map.containsKey(k2)).isFalse();
		map.put(k1, getValue());
		assertThat(map.containsKey(k1)).isTrue();
		map.put(k2, getValue());
		assertThat(map.containsKey(k2)).isTrue();
	}

	@Test
	void testContainsValue() {
		V v1 = getValue();

		assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> map.containsValue(v1));
	}

	@Test
	void testEquals() {
		RedisStore clone = copyStore(map);
		assertThat(map).isEqualTo(clone);
		assertThat(clone).isEqualTo(clone);
		assertThat(map).isEqualTo(map);
	}

	@Test
	void testNotEquals() {
		RedisOperations<String, ?> ops = map.getOperations();
		RedisStore newInstance = new DefaultRedisMap<>(ops.<K, V> boundHashOps(map.getKey() + ":new"));
		assertThat(map.equals(newInstance)).isFalse();
		assertThat(newInstance.equals(map)).isFalse();
	}

	@Test
	void testGet() {
		K k1 = getKey();
		V v1 = getValue();

		assertThat(map.get(k1)).isNull();
		map.put(k1, v1);
		assertThat(map.get(k1)).isEqualTo(v1);
	}

	@Test
	void testGetKey() {
		assertThat(map.getKey()).isNotNull();
	}

	@Test
	public void testGetOperations() {
		assertThat(map.getOperations()).isEqualTo(template);
	}

	@Test
	void testHashCode() {
		assertThat(map.hashCode()).isNotEqualTo(map.getKey().hashCode());
		assertThat(copyStore(map).hashCode()).isEqualTo(map.hashCode());
	}

	@Test
	void testIncrementNotNumber() {
		assumeThat(!(valueFactory instanceof LongAsStringObjectFactory)).isTrue();
		K k1 = getKey();
		V v1 = getValue();

		map.put(k1, v1);
		try {
			Long value = map.increment(k1, 1);
		} catch (InvalidDataAccessApiUsageException ex) {
			// expected
		} catch (RedisSystemException ex) {
			// expected for SRP and Lettuce
		}
	}

	@Test
	void testIncrement() {
		assumeThat(valueFactory instanceof LongAsStringObjectFactory).isTrue();
		K k1 = getKey();
		V v1 = getValue();
		map.put(k1, v1);
		assertThat(map.increment(k1, 10)).isEqualTo(Long.valueOf(Long.valueOf((String) v1) + 10));
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void testExpire() {

		K k1 = getKey();
		V v1 = getValue();
		assertThat(map.put(k1, v1)).isEqualTo(null);

		BoundHashFieldExpirationOperations<K> ops = map.hashFieldExpiration(Collections.singletonList(k1));
		assertThat(ops.expire(Duration.ofSeconds(5))).satisfies(ExpireChanges::allOk);
		assertThat(ops.getTimeToLive()).satisfies(expiration -> {
			assertThat(expiration.expirationOf(k1).raw()).isBetween(1L, 5L);
		});
		assertThat(ops.getTimeToLive(TimeUnit.MILLISECONDS)).satisfies(expiration -> {
			assertThat(expiration.expirationOf(k1).raw()).isBetween(1000L, 5000L);
		});
		assertThat(ops.persist()).satisfies(ExpireChanges::allOk);
	}

	@Test // GH-3054
	@EnabledOnCommand("HEXPIRE")
	void testExpireAt() {

		K k1 = getKey();
		V v1 = getValue();
		assertThat(map.put(k1, v1)).isEqualTo(null);

		BoundHashFieldExpirationOperations<K> ops = map.hashFieldExpiration(Collections.singletonList(k1));
		assertThat(ops.expireAt(Instant.now().plusSeconds(5))).satisfies(ExpireChanges::allOk);
		assertThat(ops.getTimeToLive()).satisfies(expiration -> {
			assertThat(expiration.expirationOf(k1).raw()).isBetween(1L, 5L);
		});
		assertThat(ops.getTimeToLive(TimeUnit.MILLISECONDS)).satisfies(expiration -> {
			assertThat(expiration.expirationOf(k1).raw()).isBetween(1000L, 5000L);
		});
		assertThat(ops.persist()).satisfies(ExpireChanges::allOk);
	}

	@Test
	void testIncrementDouble() {
		assumeThat(valueFactory instanceof DoubleAsStringObjectFactory).isTrue();
		K k1 = getKey();
		V v1 = getValue();
		map.put(k1, v1);
		DecimalFormat twoDForm = new DecimalFormat("#.##");
		assertThat(twoDForm.format(map.increment(k1, 3.4))).isEqualTo(twoDForm.format(Double.valueOf((String) v1) + 3.4));
	}

	@Test
	void testIsEmpty() {
		map.clear();
		assertThat(map.isEmpty()).isTrue();
		map.put(getKey(), getValue());
		assertThat(map.isEmpty()).isFalse();
		map.clear();
		assertThat(map.isEmpty()).isTrue();
	}

	@SuppressWarnings("unchecked")
	@Test
	void testKeySet() {
		map.clear();
		assertThat(map.keySet().isEmpty()).isTrue();
		K k1 = getKey();
		K k2 = getKey();
		K k3 = getKey();

		map.put(k1, getValue());
		map.put(k2, getValue());
		map.put(k3, getValue());

		Set<K> keySet = map.keySet();
		assertThat(keySet).contains(k1, k2, k3);
		assertThat(keySet.size()).isEqualTo(3);
	}

	@Test
	void testPut() {
		K k1 = getKey();
		K k2 = getKey();
		V v1 = getValue();
		V v2 = getValue();

		map.put(k1, v1);
		map.put(k2, v2);

		assertThat(map.get(k1)).isEqualTo(v1);
		assertThat(map.get(k2)).isEqualTo(v2);
	}

	@Test
	void testPutAll() {

		Map<K, V> m = new LinkedHashMap<>();
		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		m.put(k1, v1);
		m.put(k2, v2);

		assertThat(map.get(k1)).isNull();
		assertThat(map.get(k2)).isNull();

		map.putAll(m);

		assertThat(map.get(k1)).isEqualTo(v1);
		assertThat(map.get(k2)).isEqualTo(v2);
	}

	@Test
	void testRemove() {
		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		assertThat(map.remove(k1)).isNull();
		assertThat(map.remove(k2)).isNull();

		map.put(k1, v1);
		map.put(k2, v2);

		assertThat(map.remove(k1)).isEqualTo(v1);
		assertThat(map.remove(k1)).isNull();
		assertThat(map.get(k1)).isNull();

		assertThat(map.remove(k2)).isEqualTo(v2);
		assertThat(map.remove(k2)).isNull();
		assertThat(map.get(k2)).isNull();
	}

	@Test
	void testSize() {
		assertThat(map.size()).isEqualTo(0);
		map.put(getKey(), getValue());
		assertThat(map.size()).isEqualTo(1);
		K k = getKey();
		map.put(k, getValue());
		assertThat(map.size()).isEqualTo(2);
		map.remove(k);
		assertThat(map.size()).isEqualTo(1);

		map.clear();
		assertThat(map.size()).isEqualTo(0);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testValues() {
		V v1 = getValue();
		V v2 = getValue();
		V v3 = getValue();

		map.put(getKey(), v1);
		map.put(getKey(), v2);

		Collection<V> values = map.values();
		assertThat(values.size()).isEqualTo(2);
		assertThat(values).contains(v1, v2);

		map.put(getKey(), v3);
		values = map.values();
		assertThat(values.size()).isEqualTo(3);
		assertThat(values).contains(v1, v2, v3);
	}

	@SuppressWarnings("unchecked")
	@Test
	void testEntrySet() {

		Set<Entry<K, V>> entries = map.entrySet();
		assertThat(entries.isEmpty()).isTrue();

		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		map.put(k1, v1);
		map.put(k2, v1);

		entries = map.entrySet();

		Set<K> keys = new LinkedHashSet<>();
		Collection<V> values = new ArrayList<>();

		for (Entry<K, V> entry : entries) {
			keys.add(entry.getKey());
			values.add(entry.getValue());
		}

		assertThat(keys.size()).isEqualTo(2);

		assertThat(keys).contains(k1, k2);
		assertThat(values).contains(v1);
		assertThat(values).doesNotContain(v2);
	}

	@Test
	void testPutIfAbsent() {

		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		assertThat(map.get(k1)).isNull();
		assertThat(map.putIfAbsent(k1, v1)).isNull();
		assertThat(map.putIfAbsent(k1, v2)).isEqualTo(v1);
		assertThat(map.get(k1)).isEqualTo(v1);

		assertThat(map.putIfAbsent(k2, v2)).isNull();
		assertThat(map.putIfAbsent(k2, v1)).isEqualTo(v2);

		assertThat(map.get(k2)).isEqualTo(v2);
	}

	@Test
	void testConcurrentRemove() {

		K k1 = getKey();
		V v1 = getValue();
		V v2 = getValue();
		// No point testing this with byte[], they will never be equal
		assumeThat(!(v1 instanceof byte[])).isTrue();
		map.put(k1, v2);
		assertThat(map.remove(k1, v1)).isFalse();
		assertThat(map.get(k1)).isEqualTo(v2);
		assertThat(map.remove(k1, v2)).isTrue();
		assertThat(map.get(k1)).isNull();
	}

	@Test
	void testRemoveNullValue() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> map.remove(getKey(), null));
	}

	@Test
	void testConcurrentReplaceTwoArgs() {

		K k1 = getKey();
		V v1 = getValue();
		V v2 = getValue();
		// No point testing binary data here, as equals will always be false
		assumeThat(!(v1 instanceof byte[])).isTrue();

		map.put(k1, v1);

		assertThat(map.replace(k1, v2, v1)).isFalse();
		assertThat(map.get(k1)).isEqualTo(v1);
		assertThat(map.replace(k1, v1, v2)).isTrue();
		assertThat(map.get(k1)).isEqualTo(v2);
	}

	@Test
	void testReplaceNullOldValue() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> map.replace(getKey(), null, getValue()));
	}

	@Test
	void testReplaceNullNewValue() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> map.replace(getKey(), getValue(), null));
	}

	@Test
	void testConcurrentReplaceOneArg() {

		K k1 = getKey();
		V v1 = getValue();
		V v2 = getValue();

		assertThat(map.replace(k1, v1)).isNull();
		map.put(k1, v1);
		assertThat(map.replace(getKey(), v1)).isNull();
		assertThat(map.replace(k1, v2)).isEqualTo(v1);
		assertThat(map.get(k1)).isEqualTo(v2);
	}

	@Test
	void testReplaceNullValue() {
		assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> map.replace(getKey(), null));
	}

	@Test // DATAREDIS-314
	public void testScanWorksCorrectly() throws IOException {

		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		map.put(k1, v1);
		map.put(k2, v2);

		Cursor<Entry<K, V>> cursor = (Cursor<Entry<K, V>>) map.scan();
		while (cursor.hasNext()) {
			Entry<K, V> entry = cursor.next();
			assertThat(entry.getKey()).isIn(k1, k2);
			assertThat(entry.getValue()).isIn(v1, v2);
		}
		cursor.close();
	}

	@Test // GH-2048
	@EnabledOnCommand("HRANDFIELD")
	public void randomKeyFromHash() {

		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		map.put(k1, v1);
		map.put(k2, v2);

		assertThat(map.randomKey()).isIn(k1, k2);
	}

	@Test // GH-2048
	@EnabledOnCommand("HRANDFIELD")
	public void randomEntryFromHash() {

		Assumptions.assumeThat(this.valueFactory).isNotInstanceOf(RawObjectFactory.class);

		K k1 = getKey();
		K k2 = getKey();

		V v1 = getValue();
		V v2 = getValue();

		map.put(k1, v1);
		map.put(k2, v2);

		assertThat(map.randomEntry()).isIn(new AbstractMap.SimpleImmutableEntry(k1, v1),
				new AbstractMap.SimpleImmutableEntry(k2, v2));
	}

}
