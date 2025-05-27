/*
 * Copyright 2014-2025 the original author or authors.
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
package org.springframework.data.redis.connection;

import java.util.Properties;

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Franjo Zilic
 * @since 1.4
 */
public class RedisServer extends RedisNode {

	public static enum INFO {

		NAME("name"), //
		HOST("ip"), //
		PORT("port"), //
		RUN_ID("runid"), //
		FLAGS("flags"), //
		PENDING_COMMANDS("pending-commands"), //
		LAST_PING_SENT("last-ping-sent"), //
		LAST_OK_PING_REPLY("last-ok-ping-reply"), //
		DOWN_AFTER_MILLISECONDS("down-after-milliseconds"), //
		INFO_REFRESH("info-refresh"), //
		ROLE_REPORTED("role-reported"), //
		ROLE_REPORTED_TIME("role-reported-time"), //
		CONFIG_EPOCH("config-epoch"), //
		NUMBER_SLAVES("num-slaves"), //
		NUMBER_OTHER_SENTINELS("num-other-sentinels"), //
		BUFFER_LENGTH("qbuf"), //
		BUFFER_FREE_SPACE("qbuf-free"), //
		OUTPUT_BUFFER_LENGTH("obl"), //
		OUTPUT_LIST_LENGTH("number-other-sentinels"), //
		QUORUM("quorum"), //
		FAILOVER_TIMEOUT("failover-timeout"), //
		PARALLEL_SYNCS("parallel-syncs"); //

		String key;

		INFO(String key) {
			this.key = key;
		}
	}

	private Properties properties;

	/**
	 * Creates a new {@link RedisServer} with the given {@code host}, {@code port}.
	 *
	 * @param host must not be {@literal null}
	 * @param port
	 */
	public RedisServer(String host, int port) {
		this(host, port, new Properties());
	}

	/**
	 * Creates a new {@link RedisServer} with the given {@code host}, {@code port} and {@code properties}.
	 *
	 * @param host must not be {@literal null}
	 * @param port
	 * @param properties may be {@literal null}
	 */
	public RedisServer(String host, int port, @Nullable Properties properties) {

		super(host, port);
		this.properties = properties != null ? properties : new Properties();

		String name = host + ":" + port;
		if (properties != null && properties.containsKey(INFO.NAME.key)) {
			name = get(INFO.NAME);
		}

		setName(name);
	}

	/**
	 * Creates a new {@link RedisServer} from the given properties.
	 *
	 * @param properties
	 * @return
	 */
	public static RedisServer newServerFrom(Properties properties) {

		String host = properties.getProperty(INFO.HOST.key, "127.0.0.1");
		int port = Integer.parseInt(properties.getProperty(INFO.PORT.key, "26379"));

		return new RedisServer(host, port, properties);
	}

	public void setQuorum(@Nullable Long quorum) {

		if (quorum == null) {
			this.properties.remove(INFO.QUORUM.key);
			return;
		}

		this.properties.put(INFO.QUORUM.key, quorum.toString());
	}

	public @Nullable String getRunId() {
		return get(INFO.RUN_ID);
	}

	public @Nullable String getFlags() {
		return get(INFO.FLAGS);
	}

	public boolean isMaster() {

		String role = getRoleReported();
		if (!StringUtils.hasText(role)) {
			return false;
		}
		return role.equalsIgnoreCase("master");
	}

	public @Nullable Long getPendingCommands() {
		return getLongValueOf(INFO.PENDING_COMMANDS);
	}

	public @Nullable Long getLastPingSent() {
		return getLongValueOf(INFO.LAST_PING_SENT);
	}

	public @Nullable Long getLastOkPingReply() {
		return getLongValueOf(INFO.LAST_OK_PING_REPLY);
	}

	public @Nullable Long getDownAfterMilliseconds() {
		return getLongValueOf(INFO.DOWN_AFTER_MILLISECONDS);
	}

	public @Nullable Long getInfoRefresh() {
		return getLongValueOf(INFO.INFO_REFRESH);
	}

	public @Nullable String getRoleReported() {
		return get(INFO.ROLE_REPORTED);
	}

	public @Nullable Long roleReportedTime() {
		return getLongValueOf(INFO.ROLE_REPORTED_TIME);
	}

	public @Nullable Long getConfigEpoch() {
		return getLongValueOf(INFO.CONFIG_EPOCH);
	}

	/**
	 * Get the number of connected replicas.
	 *
	 * @return
	 * @since 2.1
	 */
	public @Nullable Long getNumberReplicas() {
		return getLongValueOf(INFO.NUMBER_SLAVES);
	}

	public @Nullable Long getNumberOtherSentinels() {
		return getLongValueOf(INFO.NUMBER_OTHER_SENTINELS);
	}

	public @Nullable Long getQuorum() {
		return getLongValueOf(INFO.QUORUM);
	}

	public @Nullable Long getFailoverTimeout() {
		return getLongValueOf(INFO.FAILOVER_TIMEOUT);
	}

	public @Nullable Long getParallelSyncs() {
		return getLongValueOf(INFO.PARALLEL_SYNCS);
	}

	/**
	 * @param info must not be null
	 * @return {@literal null} if no entry found for requested {@link INFO}.
	 */
	public @Nullable String get(INFO info) {

		Assert.notNull(info, "Cannot retrieve client information for 'null'");
		return get(info.key);
	}

	/**
	 * @param key must not be {@literal null} or {@literal empty}.
	 * @return {@literal null} if no entry found for requested {@code key}.
	 */
	public @Nullable String get(String key) {

		Assert.hasText(key, "Cannot get information for 'empty' / 'null' key.");
		return this.properties.getProperty(key);
	}

	private @Nullable Long getLongValueOf(INFO info) {

		String value = get(info);
		return value == null ? null : Long.valueOf(value);
	}
}
