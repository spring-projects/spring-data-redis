/*
 * Copyright 2014-2018 the original author or authors.
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
package org.springframework.data.redis.core.types;

import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link RedisClientInfo} provides general and statistical information about client connections.
 *
 * @author Christoph Strobl
 * @since 1.3
 */
@EqualsAndHashCode
public class RedisClientInfo {

	public enum INFO {

		ADDRESS_PORT("addr"), FILE_DESCRIPTOR("fd"), CONNECTION_NAME("name"), CONNECTION_AGE("age"), //
		CONNECTION_IDLE("idle"), FLAGS("flags"), DATABSE_ID("db"), CHANNEL_SUBSCRIBTIONS("sub"), //
		PATTERN_SUBSCRIBTIONS("psub"), MULIT_COMMAND_CONTEXT("multi"), BUFFER_LENGTH("qbuf"), //
		BUFFER_FREE_SPACE("qbuf-free"), OUTPUT_BUFFER_LENGTH("obl"), OUTPUT_LIST_LENGTH("oll"), //
		OUTPUT_BUFFER_MEMORY_USAGE("omem"), EVENTS("events"), LAST_COMMAND("cmd");

		final String key;

		INFO(String key) {
			this.key = key;
		}

	}

	private final Properties clientProperties;

	/**
	 * Create {@link RedisClientInfo} from {@link Properties}.
	 *
	 * @param properties must not be {@literal null}.
	 */
	public RedisClientInfo(Properties properties) {

		Assert.notNull(properties, "Cannot initialize client information for given 'null' properties.");

		this.clientProperties = new Properties();
		this.clientProperties.putAll(properties);
	}

	/**
	 * Get address/port of the client.
	 *
	 * @return
	 */
	public String getAddressPort() {
		return get(INFO.ADDRESS_PORT);
	}

	/**
	 * Get file descriptor corresponding to the socket
	 *
	 * @return
	 */
	public String getFileDescriptor() {
		return get(INFO.FILE_DESCRIPTOR);
	}

	/**
	 * Get the clients name.
	 *
	 * @return
	 */
	public String getName() {
		return get(INFO.CONNECTION_NAME);
	}

	/**
	 * Get total duration of the connection in seconds.
	 *
	 * @return
	 */
	public Long getAge() {
		return getLongValueOf(INFO.CONNECTION_AGE);
	}

	/**
	 * Get idle time of the connection in seconds.
	 *
	 * @return
	 */
	public Long getIdle() {
		return getLongValueOf(INFO.CONNECTION_IDLE);
	}

	/**
	 * Get client flags.
	 *
	 * @return
	 */
	public String getFlags() {
		return get(INFO.FLAGS);
	}

	/**
	 * Get current database index.
	 *
	 * @return
	 */
	public Long getDatabaseId() {
		return getLongValueOf(INFO.DATABSE_ID);
	}

	/**
	 * Get number of channel subscriptions.
	 *
	 * @return
	 */
	public Long getChannelSubscribtions() {
		return getLongValueOf(INFO.CHANNEL_SUBSCRIBTIONS);
	}

	/**
	 * Get number of pattern subscriptions.
	 *
	 * @return
	 */
	public Long getPatternSubscrbtions() {
		return getLongValueOf(INFO.PATTERN_SUBSCRIBTIONS);
	}

	/**
	 * Get the number of commands in a MULTI/EXEC context.
	 *
	 * @return
	 */
	public Long getMultiCommandContext() {
		return getLongValueOf(INFO.MULIT_COMMAND_CONTEXT);
	}

	/**
	 * Get the query buffer length.
	 *
	 * @return
	 */
	public Long getBufferLength() {
		return getLongValueOf(INFO.BUFFER_LENGTH);
	}

	/**
	 * Get the free space of the query buffer.
	 *
	 * @return
	 */
	public Long getBufferFreeSpace() {
		return getLongValueOf(INFO.BUFFER_FREE_SPACE);
	}

	/**
	 * Get the output buffer length.
	 *
	 * @return
	 */
	public Long getOutputBufferLength() {
		return getLongValueOf(INFO.OUTPUT_BUFFER_LENGTH);
	}

	/**
	 * Get number queued replies in output buffer.
	 *
	 * @return
	 */
	public Long getOutputListLength() {
		return getLongValueOf(INFO.OUTPUT_LIST_LENGTH);
	}

	/**
	 * Get output buffer memory usage.
	 *
	 * @return
	 */
	public Long getOutputBufferMemoryUsage() {
		return getLongValueOf(INFO.OUTPUT_BUFFER_MEMORY_USAGE);
	}

	/**
	 * Get file descriptor events.
	 *
	 * @return
	 */
	public String getEvents() {
		return get(INFO.EVENTS);
	}

	/**
	 * Get last command played.
	 *
	 * @return
	 */
	public String getLastCommand() {
		return get(INFO.LAST_COMMAND);
	}

	/**
	 * @param info must not be null
	 * @return {@literal null} if no entry found for requested {@link INFO}.
	 */
	public String get(INFO info) {

		Assert.notNull(info, "Cannot retrieve client information for 'null'.");
		return this.clientProperties.getProperty(info.key);
	}

	/**
	 * @param key must not be {@literal null} or {@literal empty}.
	 * @return {@literal null} if no entry found for requested {@code key}.
	 */
	@Nullable
	public String get(String key) {

		Assert.hasText(key, "Cannot get client information for 'empty' / 'null' key.");
		return this.clientProperties.getProperty(key);
	}

	private Long getLongValueOf(INFO info) {

		String value = get(info);
		return value == null ? null : Long.valueOf(value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.clientProperties.toString();
	}

	public static class RedisClientInfoBuilder {

		public static RedisClientInfo fromString(String source) {

			Assert.notNull(source, "Cannot read client properties form 'null'.");
			Properties properties = new Properties();
			try {
				properties.load(new StringReader(source.replace(' ', '\n')));
			} catch (IOException e) {
				throw new IllegalArgumentException(String.format("Properties could not be loaded from String '%s'.", source),
						e);
			}
			return new RedisClientInfo(properties);
		}
	}
}
