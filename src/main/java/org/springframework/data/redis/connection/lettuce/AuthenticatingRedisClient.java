/*
 * Copyright 2013-2015 the original author or authors.
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

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;

/**
 * Extension of {@link RedisClient} that calls auth on all new connections using the supplied credentials
 * 
 * @author Jennifer Hickey
 * @author Mar Paluch
 * @author Christoph Strobl
 * @deprecated since 1.6 - Please use {@link RedisURI#setPassword(String)}
 */
@Deprecated
public class AuthenticatingRedisClient extends RedisClient {

	private String password;

	public AuthenticatingRedisClient(String host, int port, String password) {
		super(host, port);
		this.password = password;
	}

	public AuthenticatingRedisClient(String host, String password) {
		super(host);
		this.password = password;
	}

	@Override
	public <K, V> RedisConnection<K, V> connect(RedisCodec<K, V> codec) {
		RedisConnection<K, V> conn = super.connect(codec);
		conn.auth(password);
		return conn;
	}

	@Override
	public <K, V> RedisAsyncConnection<K, V> connectAsync(RedisCodec<K, V> codec) {
		RedisAsyncConnection<K, V> conn = super.connectAsync(codec);
		conn.auth(password);
		return conn;
	}

	@Override
	public <K, V> RedisPubSubConnection<K, V> connectPubSub(RedisCodec<K, V> codec) {
		RedisPubSubConnection<K, V> conn = super.connectPubSub(codec);
		conn.auth(password);
		return conn;
	}
}
