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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.ExpirationOptions;
import org.springframework.data.redis.connection.ReactiveHashCommands;
import org.springframework.data.redis.connection.ReactiveHashCommands.HashExpireCommand;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.data.redis.core.types.Expirations;
import org.springframework.data.redis.core.types.Expirations.Timeouts;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.util.Assert;

/**
 * Default implementation of {@link ReactiveHashOperations}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author John Blum
 * @since 2.0
 */
class DefaultReactiveHashOperations<H, HK, HV> implements ReactiveHashOperations<H, HK, HV> {

	private final ReactiveRedisTemplate<?, ?> template;
	private final RedisSerializationContext<H, ?> serializationContext;

	DefaultReactiveHashOperations(ReactiveRedisTemplate<?, ?> template,
			RedisSerializationContext<H, ?> serializationContext) {

		this.template = template;
		this.serializationContext = serializationContext;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Long> remove(H key, Object... hashKeys) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashKeys, "Hash keys must not be null");
		Assert.notEmpty(hashKeys, "Hash keys must not be empty");
		Assert.noNullElements(hashKeys, "Hash keys must not contain null elements");

		return createMono(hashCommands -> Flux.fromArray(hashKeys) //
				.map(hashKey -> (HK) hashKey).map(this::rawHashKey) //
				.collectList() //
				.flatMap(hks -> hashCommands.hDel(rawKey(key), hks)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<Boolean> hasKey(H key, Object hashKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashKey, "Hash key must not be null");

		return createMono(hashCommands -> hashCommands.hExists(rawKey(key), rawHashKey((HK) hashKey)));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Mono<HV> get(H key, Object hashKey) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashKey, "Hash key must not be null");

		return createMono(
				hashCommands -> hashCommands.hGet(rawKey(key), rawHashKey((HK) hashKey)).map(this::readHashValue));
	}

	@Override
	public Mono<List<HV>> multiGet(H key, Collection<HK> hashKeys) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashKeys, "Hash keys must not be null");
		Assert.notEmpty(hashKeys, "Hash keys must not be empty");

		return createMono(hashCommands -> Flux.fromIterable(hashKeys) //
				.map(this::rawHashKey) //
				.collectList() //
				.flatMap(hks -> hashCommands.hMGet(rawKey(key), hks)).map(this::deserializeHashValues));
	}

    @Override
    public Mono<List<HV>> getAndDelete(H key, Collection<HK> hashKeys) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(hashKeys, "Hash keys must not be null");
        Assert.notEmpty(hashKeys, "Hash keys must not be empty");

        return createMono(hashCommands -> Flux.fromIterable(hashKeys) //
                .map(this::rawHashKey) //
                .collectList() //
                .flatMap(hks -> hashCommands.hGetDel(rawKey(key), hks)).map(this::deserializeHashValues));
    }

    @Override
    public Mono<Boolean> putAndExpire(H key, Map<? extends HK, ? extends HV> map, RedisHashCommands.HashFieldSetOption condition, Expiration expiration) {
        Assert.notNull(key, "Key must not be null");
        Assert.notNull(map, "Map must not be null");

        return createMono(hashCommands -> Flux.fromIterable(() -> map.entrySet().iterator()) //
                .collectMap(entry -> rawHashKey(entry.getKey()), entry -> rawHashValue(entry.getValue())) //
                .flatMap(serialized -> hashCommands.hSetEx(rawKey(key), serialized, condition, expiration)));
    }

    @Override
    public Mono<List<HV>> getAndExpire(H key, Expiration expiration, Collection<HK> hashKeys) {

        Assert.notNull(key, "Key must not be null");
        Assert.notNull(hashKeys, "Hash keys must not be null");
        Assert.notEmpty(hashKeys, "Hash keys must not be empty");

        return createMono(hashCommands -> Flux.fromIterable(hashKeys) //
                .map(this::rawHashKey) //
                .collectList() //
                .flatMap(hks -> hashCommands.hGetEx(rawKey(key), expiration, hks)).map(this::deserializeHashValues));
    }

	@Override
	public Mono<Long> increment(H key, HK hashKey, long delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashKey, "Hash key must not be null");

		return template
				.doCreateMono(connection -> connection.numberCommands().hIncrBy(rawKey(key), rawHashKey(hashKey), delta));
	}

	@Override
	public Mono<Double> increment(H key, HK hashKey, double delta) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashKey, "Hash key must not be null");

		return template
				.doCreateMono(connection -> connection.numberCommands().hIncrBy(rawKey(key), rawHashKey(hashKey), delta));
	}

	@Override
	public Mono<HK> randomKey(H key) {

		Assert.notNull(key, "Key must not be null");

		return template.doCreateMono(connection -> connection.hashCommands().hRandField(rawKey(key)))
				.map(this::readRequiredHashKey);
	}

	@Override
	public Mono<Map.Entry<HK, HV>> randomEntry(H key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(hashCommands -> hashCommands.hRandFieldWithValues(rawKey(key))).map(this::deserializeHashEntry);
	}

	@Override
	public Flux<HK> randomKeys(H key, long count) {

		Assert.notNull(key, "Key must not be null");

		return template.doCreateFlux(connection -> connection.hashCommands().hRandField(rawKey(key), count))
				.map(this::readRequiredHashKey);
	}

	@Override
	public Flux<Map.Entry<HK, HV>> randomEntries(H key, long count) {

		Assert.notNull(key, "Key must not be null");

		return template.doCreateFlux(connection -> connection.hashCommands().hRandFieldWithValues(rawKey(key), count))
				.map(this::deserializeHashEntry);
	}

	@Override
	public Flux<HK> keys(H key) {

		Assert.notNull(key, "Key must not be null");

		return createFlux(connection -> connection.hKeys(rawKey(key)) //
				.map(this::readRequiredHashKey));
	}

	@Override
	public Mono<Long> size(H key) {

		Assert.notNull(key, "Key must not be null");

		return createMono(hashCommands -> hashCommands.hLen(rawKey(key)));
	}

	@Override
	public Mono<Boolean> putAll(H key, Map<? extends HK, ? extends HV> map) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(map, "Map must not be null");

		return createMono(hashCommands -> Flux.fromIterable(() -> map.entrySet().iterator()) //
				.collectMap(entry -> rawHashKey(entry.getKey()), entry -> rawHashValue(entry.getValue())) //
				.flatMap(serialized -> hashCommands.hMSet(rawKey(key), serialized)));
	}

	@Override
	public Mono<Boolean> put(H key, HK hashKey, HV value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashKey, "Hash key must not be null");
		Assert.notNull(value, "Hash value must not be null");

		return createMono(hashCommands -> hashCommands.hSet(rawKey(key), rawHashKey(hashKey), rawHashValue(value)));
	}

	@Override
	public Mono<Boolean> putIfAbsent(H key, HK hashKey, HV value) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(hashKey, "Hash key must not be null");
		Assert.notNull(value, "Hash value must not be null");

		return createMono(hashCommands -> hashCommands.hSetNX(rawKey(key), rawHashKey(hashKey), rawHashValue(value)));
	}

	@Override
	public Flux<HV> values(H key) {

		Assert.notNull(key, "Key must not be null");

		return createFlux(hashCommands -> hashCommands.hVals(rawKey(key)) //
				.map(this::readRequiredHashValue));
	}

	@Override
	public Flux<Map.Entry<HK, HV>> entries(H key) {

		Assert.notNull(key, "Key must not be null");

		return createFlux(hashCommands -> hashCommands.hGetAll(rawKey(key)) //
				.map(this::deserializeHashEntry));
	}

	@Override
	public Flux<Map.Entry<HK, HV>> scan(H key, ScanOptions options) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(key, "ScanOptions must not be null");

		return createFlux(hashCommands -> hashCommands.hScan(rawKey(key), options) //
				.map(this::deserializeHashEntry));
	}

	@Override
	public Mono<ExpireChanges<HK>> expire(H key, Duration timeout, Collection<HK> hashKeys) {
		return expire(key, Expiration.from(timeout), ExpirationOptions.none(), hashKeys);
	}

	@Override
	public Mono<ExpireChanges<HK>> expire(H key, Expiration expiration, ExpirationOptions options,
			Collection<HK> hashKeys) {

		List<HK> orderedKeys = List.copyOf(hashKeys);
		ByteBuffer rawKey = rawKey(key);
		List<ByteBuffer> rawHashKeys = orderedKeys.stream().map(this::rawHashKey).toList();

		Mono<List<Long>> raw = createFlux(connection -> {
			return connection
					.applyHashFieldExpiration(
							Mono.just(HashExpireCommand.expire(rawHashKeys, expiration).from(rawKey).withOptions(options)))
					.map(NumericResponse::getOutput);
		}).collectList();

		return raw.map(values -> ExpireChanges.of(orderedKeys, values));
	}

	@Nullable
	@Override
	public Mono<ExpireChanges<HK>> expireAt(H key, Instant expireAt, Collection<HK> hashKeys) {

		List<HK> orderedKeys = List.copyOf(hashKeys);
		ByteBuffer rawKey = rawKey(key);
		List<ByteBuffer> rawHashKeys = orderedKeys.stream().map(this::rawHashKey).toList();

		Mono<List<Long>> raw = createFlux(connection -> connection.hExpireAt(rawKey, expireAt, rawHashKeys)).collectList();

		return raw.map(values -> ExpireChanges.of(orderedKeys, values));
	}

	@Nullable
	@Override
	public Mono<ExpireChanges<HK>> persist(H key, Collection<HK> hashKeys) {

		List<HK> orderedKeys = List.copyOf(hashKeys);
		ByteBuffer rawKey = rawKey(key);
		List<ByteBuffer> rawHashKeys = orderedKeys.stream().map(this::rawHashKey).toList();

		Mono<List<Long>> raw = createFlux(connection -> connection.hPersist(rawKey, rawHashKeys)).collectList();

		return raw.map(values -> ExpireChanges.of(orderedKeys, values));
	}

	@Nullable
	@Override
	public Mono<Expirations<HK>> getTimeToLive(H key, TimeUnit timeUnit, Collection<HK> hashKeys) {

		if (timeUnit.compareTo(TimeUnit.MILLISECONDS) < 0) {
			throw new IllegalArgumentException("%s precision is not supported must be >= MILLISECONDS".formatted(timeUnit));
		}

		List<HK> orderedKeys = List.copyOf(hashKeys);
		ByteBuffer rawKey = rawKey(key);
		List<ByteBuffer> rawHashKeys = orderedKeys.stream().map(this::rawHashKey).toList();

		Mono<List<Long>> raw = createFlux(connection -> {

			if (TimeUnit.MILLISECONDS.equals(timeUnit)) {
				return connection.hpTtl(rawKey, rawHashKeys);
			}
			return connection.hTtl(rawKey, rawHashKeys);
		}).collectList();

		return raw.map(values -> {

			Timeouts timeouts = new Timeouts(TimeUnit.MILLISECONDS.equals(timeUnit) ? timeUnit : TimeUnit.SECONDS, values);
			return Expirations.of(timeUnit, orderedKeys, timeouts);
		});
	}

	@Override
	public Mono<Boolean> delete(H key) {

		Assert.notNull(key, "Key must not be null");

		return template.doCreateMono(connection -> connection.keyCommands().del(rawKey(key))).map(l -> l != 0);
	}

	private <T> Mono<T> createMono(Function<ReactiveHashCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null");

		return template.doCreateMono(connection -> function.apply(connection.hashCommands()));
	}

	private <T> Flux<T> createFlux(Function<ReactiveHashCommands, Publisher<T>> function) {

		Assert.notNull(function, "Function must not be null");

		return template.doCreateFlux(connection -> function.apply(connection.hashCommands()));
	}

	private ByteBuffer rawKey(H key) {
		return serializationContext.getKeySerializationPair().write(key);
	}

	private ByteBuffer rawHashKey(HK key) {
		return serializationContext.getHashKeySerializationPair().write(key);
	}

	private ByteBuffer rawHashValue(HV key) {
		return serializationContext.getHashValueSerializationPair().write(key);
	}

	@SuppressWarnings("unchecked")
	private @Nullable HK readHashKey(ByteBuffer value) {
		return (HK) serializationContext.getHashKeySerializationPair().read(value);
	}

	private HK readRequiredHashKey(ByteBuffer buffer) {

		HK hashKey = readHashKey(buffer);

		if (hashKey != null) {
			return hashKey;
		}

		throw new InvalidDataAccessApiUsageException("Deserialized hash key is null");
	}

	@SuppressWarnings("unchecked")
	private @Nullable HV readHashValue(@Nullable ByteBuffer value) {
		return value != null ? (HV) serializationContext.getHashValueSerializationPair().read(value) : null;
	}

	private HV readRequiredHashValue(ByteBuffer buffer) {

		HV hashValue = readHashValue(buffer);

		if (hashValue != null) {
			return hashValue;
		}

		throw new InvalidDataAccessApiUsageException("Deserialized hash value is null");
	}

	private Map.Entry<HK, HV> deserializeHashEntry(Map.Entry<ByteBuffer, ByteBuffer> source) {
		return Converters.entryOf(readHashKey(source.getKey()), readHashValue(source.getValue()));
	}

	private List<HV> deserializeHashValues(List<ByteBuffer> source) {

		List<HV> values = new ArrayList<>(source.size());

		for (ByteBuffer byteBuffer : source) {
			values.add(readHashValue(byteBuffer));
		}

		return values;
	}
}
