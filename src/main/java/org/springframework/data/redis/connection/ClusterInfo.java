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
package org.springframework.data.redis.connection;

import java.util.Properties;

import org.springframework.data.redis.core.types.RedisClientInfo.INFO;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link ClusterInfo} gives access to cluster information such as {@code cluster_state} and
 * {@code cluster_slots_assigned} provided by the {@code CLUSTER INFO} command.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class ClusterInfo {

	public static enum Info {
		STATE("cluster_state"), SLOTS_ASSIGNED("cluster_slots_assigned"), SLOTS_OK("cluster_slots_ok"), SLOTS_PFAIL(
				"cluster_slots_pfail"), SLOTS_FAIL("cluster_slots_fail"), KNOWN_NODES("cluster_known_nodes"), SIZE(
						"cluster_size"), CURRENT_EPOCH("cluster_current_epoch"), MY_EPOCH("cluster_my_epoch"), MESSAGES_SENT(
								"cluster_stats_messages_sent"), MESSAGES_RECEIVED("cluster_stats_messages_received");

		String key;

		Info(String key) {
			this.key = key;
		}
	}

	private final Properties clusterProperties;

	/**
	 * Creates new {@link ClusterInfo} for given {@link Properties}.
	 *
	 * @param clusterProperties must not be {@literal null}.
	 */
	public ClusterInfo(Properties clusterProperties) {

		Assert.notNull(clusterProperties, "ClusterProperties must not be null!");
		this.clusterProperties = clusterProperties;
	}

	/**
	 * @see Info#STATE
	 * @return {@literal null} if no entry found for requested {@link Info#STATE}.
	 */
	@Nullable
	public String getState() {
		return get(Info.STATE);
	}

	/**
	 * @see Info#SLOTS_ASSIGNED
	 * @return {@literal null} if no entry found for requested {@link Info#SLOTS_ASSIGNED}.
	 */
	@Nullable
	public Long getSlotsAssigned() {
		return getLongValueOf(Info.SLOTS_ASSIGNED);
	}

	/**
	 * @see Info#SLOTS_OK
	 * @return {@literal null} if no entry found for requested {@link Info#SLOTS_OK}.
	 */
	@Nullable
	public Long getSlotsOk() {
		return getLongValueOf(Info.SLOTS_OK);
	}

	/**
	 * @see Info#SLOTS_PFAIL
	 * @return {@literal null} if no entry found for requested {@link Info#SLOTS_PFAIL}.
	 */
	@Nullable
	public Long getSlotsPfail() {
		return getLongValueOf(Info.SLOTS_PFAIL);
	}

	/**
	 * @see Info#SLOTS_FAIL
	 * @return {@literal null} if no entry found for requested {@link Info#SLOTS_FAIL}.
	 */
	@Nullable
	public Long getSlotsFail() {
		return getLongValueOf(Info.SLOTS_FAIL);
	}

	/**
	 * @see Info#KNOWN_NODES
	 * @return {@literal null} if no entry found for requested {@link Info#KNOWN_NODES}.
	 */
	@Nullable
	public Long getKnownNodes() {
		return getLongValueOf(Info.KNOWN_NODES);
	}

	/**
	 * @see Info#SIZE
	 * @return {@literal null} if no entry found for requested {@link Info#SIZE}.
	 */
	@Nullable
	public Long getClusterSize() {
		return getLongValueOf(Info.SIZE);
	}

	/**
	 * @see Info#CURRENT_EPOCH
	 * @return {@literal null} if no entry found for requested {@link Info#CURRENT_EPOCH}.
	 */
	@Nullable
	public Long getCurrentEpoch() {
		return getLongValueOf(Info.CURRENT_EPOCH);
	}

	/**
	 * @see Info#MESSAGES_SENT
	 * @return {@literal null} if no entry found for requested {@link Info#MESSAGES_SENT}.
	 */
	@Nullable
	public Long getMessagesSent() {
		return getLongValueOf(Info.MESSAGES_SENT);
	}

	/**
	 * @see Info#MESSAGES_RECEIVED
	 * @return {@literal null} if no entry found for requested {@link Info#MESSAGES_RECEIVED}.
	 */
	@Nullable
	public Long getMessagesReceived() {
		return getLongValueOf(Info.MESSAGES_RECEIVED);
	}

	/**
	 * @param info must not be null
	 * @return {@literal null} if no entry found for requested {@link INFO}.
	 */
	@Nullable
	public String get(Info info) {

		Assert.notNull(info, "Cannot retrieve cluster information for 'null'.");
		return get(info.key);
	}

	/**
	 * @param key must not be {@literal null} or {@literal empty}.
	 * @return {@literal null} if no entry found for requested {@code key}.
	 */
	@Nullable
	public String get(String key) {

		Assert.hasText(key, "Cannot get cluster information for 'empty' / 'null' key.");
		return this.clusterProperties.getProperty(key);
	}

	@Nullable
	private Long getLongValueOf(Info info) {

		String value = get(info);
		return value == null ? null : Long.valueOf(value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return this.clusterProperties.toString();
	}

}
