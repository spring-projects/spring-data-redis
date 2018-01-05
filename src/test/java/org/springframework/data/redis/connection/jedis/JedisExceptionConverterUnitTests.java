/*
 * Copyright 2015-2018 the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ClusterRedirectException;
import org.springframework.data.redis.TooManyClusterRedirectionsException;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisMovedDataException;

/**
 * @author Christoph Strobl
 */
public class JedisExceptionConverterUnitTests {

	JedisExceptionConverter converter;

	@Before
	public void setUp() {
		converter = new JedisExceptionConverter();
	}

	@Test // DATAREDIS-315
	public void shouldConvertMovedDataException() {

		DataAccessException converted = converter.convert(new JedisMovedDataException("MOVED 3999 127.0.0.1:6381",
				new HostAndPort("127.0.0.1", 6381), 3999));

		assertThat(converted, instanceOf(ClusterRedirectException.class));
		assertThat(((ClusterRedirectException) converted).getSlot(), is(3999));
		assertThat(((ClusterRedirectException) converted).getTargetHost(), is("127.0.0.1"));
		assertThat(((ClusterRedirectException) converted).getTargetPort(), is(6381));
	}

	@Test // DATAREDIS-315
	public void shouldConvertAskDataException() {

		DataAccessException converted = converter.convert(new JedisAskDataException("ASK 3999 127.0.0.1:6381",
				new HostAndPort("127.0.0.1", 6381), 3999));

		assertThat(converted, instanceOf(ClusterRedirectException.class));
		assertThat(((ClusterRedirectException) converted).getSlot(), is(3999));
		assertThat(((ClusterRedirectException) converted).getTargetHost(), is("127.0.0.1"));
		assertThat(((ClusterRedirectException) converted).getTargetPort(), is(6381));
	}

	@Test // DATAREDIS-315
	public void shouldConvertMaxRedirectException() {

		DataAccessException converted = converter
				.convert(new JedisClusterMaxRedirectionsException("Too many redirections?"));

		assertThat(converted, instanceOf(TooManyClusterRedirectionsException.class));
	}
}
