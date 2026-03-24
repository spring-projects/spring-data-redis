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

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.RedisJsonCommands;

import java.util.List;

/**
 * {@link RedisJsonCommands} implementation for Jedis.
 *
 * @author Yordan Tsintsov
 * @since 4.2
 */
class JedisJsonCommands implements RedisJsonCommands {

	private final JedisConnection connection;

	JedisJsonCommands(JedisConnection connection) {
		this.connection = connection;
	}

	@Override
	public List<@Nullable Long> jsonArrAppend(byte[] key, String path, String... values) {
		return List.of();
	}

	@Override
	public List<@Nullable Long> jsonArrIndex(byte[] key, String path, String value, long start, long stop) {
		return List.of();
	}

	@Override
	public List<@Nullable Long> jsonArrInsert(byte[] key, String path, int index, String... values) {
		return List.of();
	}

	@Override
	public List<@Nullable Long> jsonArrLen(byte[] key, String path) {
		return List.of();
	}

	@Override
	public List<@Nullable String> jsonArrPop(byte[] key, String path, int index) {
		return List.of();
	}

	@Override
	public List<@Nullable Long> jsonArrTrim(byte[] key, String path, int start, int stop) {
		return List.of();
	}

	@Override
	public Long jsonClear(byte[] key, String path) {
		return 0L;
	}

	@Override
	public Long jsonDel(byte[] key, String path) {
		return 0L;
	}

	@Override
	public List<@Nullable String> jsonGet(byte[] key, String... paths) {
		return List.of();
	}

	@Override
	public Boolean jsonMerge(byte[] key, String path, String value) {
		return null;
	}

	@Override
	public List<@Nullable String> jsonMGet(String path, byte[]... keys) {
		return List.of();
	}

	@Override
	public Boolean jsonMSet(List<JsonMSetArgs> args) {
		return null;
	}

	@Override
	public List<@Nullable Number> jsonNumIncrBy(byte[] key, String path, Number number) {
		return List.of();
	}

	@Override
	public Boolean jsonSet(byte[] key, String path, String value, JsonSetOption option) {
		return null;
	}

	@Override
	public List<@Nullable Long> jsonStrAppend(byte[] key, String path, String value) {
		return List.of();
	}

	@Override
	public List<@Nullable Long> jsonStrLen(byte[] key, String path) {
		return List.of();
	}

	@Override
	public List<@Nullable Boolean> jsonToggle(byte[] key, String path) {
		return List.of();
	}

	@Override
	public List<@Nullable JsonType> jsonType(byte[] key, String path) {
		return List.of();
	}

}
