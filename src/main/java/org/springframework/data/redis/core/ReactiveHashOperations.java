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

import org.springframework.data.redis.connection.RedisHashCommands;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.Expirations;

/**
 * Reactive Redis operations for Hash Commands.
 * <p>
 * Streams of methods returning {@code Mono<K>} or {@code Flux<M>} are terminated with
 * {@link org.springframework.dao.InvalidDataAccessApiUsageException} when
 * {@link org.springframework.data.redis.serializer.RedisElementReader#read(ByteBuffer)} returns {@literal null} for a
 * particular element as Reactive Streams prohibit the usage of {@literal null} values.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Viktoriya Kutsarova
 * @since 2.0
 */
public interface ReactiveHashOperations<H, HK, HV> {

	/**
	 * Delete given hash {@code hashKeys} from the hash at {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return
	 */
	Mono<Long> remove(H key, Object... hashKeys);

	/**
	 * Determine if given hash {@code hashKey} exists.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return
	 */
	Mono<Boolean> hasKey(H key, Object hashKey);

	/**
	 * Get value for given {@code hashKey} from hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @return
	 */
	Mono<HV> get(H key, Object hashKey);

	/**
	 * Get values for given {@code hashKeys} from hash at {@code key}. Values are in the order of the requested keys.
	 * Absent field values are represented using {@literal null} in the resulting {@link List}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return
	 */
	Mono<List<HV>> multiGet(H key, Collection<HK> hashKeys);
    
    /**
     * Get and remove the value for given {@code hashKeys} from hash at {@code key}. Values are in the order of the
     * requested keys. Absent field values are represented using {@literal null} in the resulting {@link List}.
     * When the last field is deleted, the key will also be deleted.
     *
     * @param key must not be {@literal null}.
     * @param hashKeys must not be {@literal null}.
     * @return never {@literal null}.
     * @since 4.0
     */
    Mono<List<HV>> getAndDelete(H key, Collection<HK> hashKeys);

    /**
     * Set multiple hash fields to multiple values using data provided in {@code m} with optional condition and expiration.
     *
     * @param key must not be {@literal null}.
     * @param map must not be {@literal null}.
     * @param condition is optional.
     * @param expiration is optional.
     * @return never {@literal null}.
     * @since 4.0
     */
    Mono<Boolean> putAndExpire(H key, Map<? extends HK, ? extends HV> map, RedisHashCommands.HashFieldSetOption condition,
                               Expiration expiration);

    /**
     * Get and optionally expire the value for given {@code hashKeys} from hash at {@code key}. Values are in the order of the
     * requested keys. Absent field values are represented using {@literal null} in the resulting {@link List}.
     *
     * @param key must not be {@literal null}.
     * @param expiration is optional.
     * @param hashKeys must not be {@literal null}.
     * @return never {@literal null}.
     * @since 4.0
     */
    Mono<List<HV>> getAndExpire(H key, Expiration expiration, Collection<HK> hashKeys);

	/**
	 * Increment {@code value} of a hash {@code hashKey} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param delta
	 * @return
	 */
	Mono<Long> increment(H key, HK hashKey, long delta);

	/**
	 * Increment {@code value} of a hash {@code hashKey} by the given {@code delta}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param delta
	 * @return
	 */
	Mono<Double> increment(H key, HK hashKey, double delta);

	/**
	 * Return a random hash key from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Mono<HK> randomKey(H key);

	/**
	 * Return a random entry from the hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Mono<Map.Entry<HK, HV>> randomEntry(H key);

	/**
	 * Return random hash keys from the hash stored at {@code key}. If the provided {@code count} argument is positive,
	 * return a list of distinct hash keys, capped either at {@code count} or the hash size. If {@code count} is negative,
	 * the behavior changes and the command is allowed to return the same hash key multiple times. In this case, the
	 * number of returned fields is the absolute value of the specified count.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return.
	 * @return
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Flux<HK> randomKeys(H key, long count);

	/**
	 * Return random entries from the hash stored at {@code key}. If the provided {@code count} argument is positive,
	 * return a list of distinct entries, capped either at {@code count} or the hash size. If {@code count} is negative,
	 * the behavior changes and the command is allowed to return the same entry multiple times. In this case, the number
	 * of returned fields is the absolute value of the specified count.
	 *
	 * @param key must not be {@literal null}.
	 * @param count number of fields to return.
	 * @return {@literal null} if key does not exist or when used in pipeline / transaction.
	 * @since 2.6
	 * @see <a href="https://redis.io/commands/hrandfield">Redis Documentation: HRANDFIELD</a>
	 */
	Flux<Map.Entry<HK, HV>> randomEntries(H key, long count);

	/**
	 * Get key set (fields) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Flux<HK> keys(H key);

	/**
	 * Get size of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Mono<Long> size(H key);

	/**
	 * Set multiple hash fields to multiple values using data provided in {@code m}.
	 *
	 * @param key must not be {@literal null}.
	 * @param map must not be {@literal null}.
	 */
	Mono<Boolean> putAll(H key, Map<? extends HK, ? extends HV> map);

	/**
	 * Set the {@code value} of a hash {@code hashKey}.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param value
	 */
	Mono<Boolean> put(H key, HK hashKey, HV value);

	/**
	 * Set the {@code value} of a hash {@code hashKey} only if {@code hashKey} does not exist.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKey must not be {@literal null}.
	 * @param value
	 * @return
	 */
	Mono<Boolean> putIfAbsent(H key, HK hashKey, HV value);

	/**
	 * Get entry set (values) of hash at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Flux<HV> values(H key);

	/**
	 * Get entire hash stored at {@code key}.
	 *
	 * @param key must not be {@literal null}.
	 * @return
	 */
	Flux<Map.Entry<HK, HV>> entries(H key);

	/**
	 * Use a {@link Flux} to iterate over entries in the hash at {@code key}. The resulting {@link Flux} acts as a cursor
	 * and issues {@code HSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @return the {@link Flux} emitting the {@link java.util.Map.Entry entries} on by one or an {@link Flux#empty() empty
	 *         flux} if the key does not exist.
	 * @throws IllegalArgumentException when the given {@code key} is {@literal null}.
	 * @see <a href="https://redis.io/commands/hscan">Redis Documentation: HSCAN</a>
	 * @since 2.1
	 */
	default Flux<Map.Entry<HK, HV>> scan(H key) {
		return scan(key, ScanOptions.NONE);
	}

	/**
	 * Use a {@link Flux} to iterate over entries in the hash at {@code key} given {@link ScanOptions}. The resulting
	 * {@link Flux} acts as a cursor and issues {@code HSCAN} commands itself as long as the subscriber signals demand.
	 *
	 * @param key must not be {@literal null}.
	 * @param options must not be {@literal null}. Use {@link ScanOptions#NONE} instead.
	 * @return the {@link Flux} emitting the {@link java.util.Map.Entry entries} on by one or an {@link Flux#empty() empty
	 *         flux} if the key does not exist.
	 * @throws IllegalArgumentException when one of the required arguments is {@literal null}.
	 * @see <a href="https://redis.io/commands/hscan">Redis Documentation: HSCAN</a>
	 * @since 2.1
	 */
	Flux<Map.Entry<HK, HV>> scan(H key, ScanOptions options);

	/**
	 * Set time to live for given {@literal hashKeys} stored within {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeout the amount of time after which the key will be expired, must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a {@link Mono} emitting changes to the hash fields.
	 * @throws IllegalArgumentException if the timeout is {@literal null}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	Mono<ExpireChanges<HK>> expire(H key, Duration timeout, Collection<HK> hashKeys);

	/**
	 * Set time to live for given {@literal hashKeys} stored within {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 * @param expiration must not be {@literal null}.
	 * @param options additional options to apply.
	 * @param hashKeys must not be {@literal null}.
	 * @return a {@link Mono} emitting changes to the hash fields.
	 * @throws IllegalArgumentException if the timeout is {@literal null}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpire/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	Mono<ExpireChanges<HK>> expire(H key, Expiration expiration, ExpirationOptions options, Collection<HK> hashKeys);

	/**
	 * Set the expiration for given {@code hashKey} as a {@literal date} timestamp.
	 *
	 * @param key must not be {@literal null}.
	 * @param expireAt must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a {@link Mono} emitting changes to the hash fields.
	 * @throws IllegalArgumentException if the instant is {@literal null} or too large to represent as a {@code Date}.
	 * @see <a href="https://redis.io/docs/latest/commands/hexpireat/">Redis Documentation: HEXPIRE</a>
	 * @since 3.5
	 */
	@Nullable
	Mono<ExpireChanges<HK>> expireAt(H key, Instant expireAt, Collection<HK> hashKeys);

	/**
	 * Remove the expiration from given {@code hashKey} .
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a {@link Mono} emitting changes to the hash fields.
	 * @see <a href="https://redis.io/docs/latest/commands/hpersist/">Redis Documentation: HPERSIST</a>
	 * @since 3.5
	 */
	@Nullable
	Mono<ExpireChanges<HK>> persist(H key, Collection<HK> hashKeys);

	/**
	 * Get the time to live for {@code hashKey} in seconds.
	 *
	 * @param key must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a {@link Mono} emitting {@link Expirations} of the hash fields.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	default @Nullable Mono<Expirations<HK>> getTimeToLive(H key, Collection<HK> hashKeys) {
		return getTimeToLive(key, TimeUnit.SECONDS, hashKeys);
	}

	/**
	 * Get the time to live for {@code hashKey} and convert it to the given {@link TimeUnit}.
	 *
	 * @param key must not be {@literal null}.
	 * @param timeUnit must not be {@literal null}.
	 * @param hashKeys must not be {@literal null}.
	 * @return a {@link Mono} emitting {@link Expirations} of the hash fields.
	 * @see <a href="https://redis.io/docs/latest/commands/httl/">Redis Documentation: HTTL</a>
	 * @since 3.5
	 */
	@Nullable
	Mono<Expirations<HK>> getTimeToLive(H key, TimeUnit timeUnit, Collection<HK> hashKeys);

	/**
	 * Removes the given {@literal key}.
	 *
	 * @param key must not be {@literal null}.
	 */
	Mono<Boolean> delete(H key);

}
