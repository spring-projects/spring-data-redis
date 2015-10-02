/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.redis.core.convert;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 */
public class KeyspaceConfiguration {

	private Map<Class<?>, KeyspaceSettings> settingsMap;

	public KeyspaceConfiguration() {

		this.settingsMap = new ConcurrentHashMap<Class<?>, KeyspaceSettings>();
		for (KeyspaceSettings initial : initialConfiguration()) {
			settingsMap.put(initial.type, initial);
		}
	}

	public boolean hasSettingsFor(Class<?> type) {

		if (settingsMap.containsKey(type)) {

			if (settingsMap.get(type) instanceof DefaultKeyspaceSetting) {
				return false;
			}

			return true;
		}

		for (KeyspaceSettings assignment : settingsMap.values()) {
			if (assignment.inherit) {
				if (ClassUtils.isAssignable(assignment.type, type)) {
					settingsMap.put(type, assignment.cloneFor(type));
					return true;
				}
			}
		}

		settingsMap.put(type, new DefaultKeyspaceSetting(type));
		return false;
	}

	public KeyspaceSettings getKeyspaceSettings(Class<?> type) {

		KeyspaceSettings settings = settingsMap.get(type);
		if (settings == null || settings instanceof DefaultKeyspaceSetting) {
			return null;
		}

		return settings;
	}

	/**
	 * Customization hook.
	 * 
	 * @return must not return {@literal null}.
	 */
	protected Iterable<KeyspaceSettings> initialConfiguration() {
		return Collections.emptySet();
	}

	public static class KeyspaceSettings {

		private final String keyspace;
		private final Class<?> type;
		private final boolean inherit;
		private Long timeToLive;

		public KeyspaceSettings(Class<?> type, String keyspace) {
			this(type, keyspace, true);
		}

		public KeyspaceSettings(Class<?> type, String keyspace, boolean inherit) {

			this.type = type;
			this.keyspace = keyspace;
			this.inherit = inherit;
		}

		KeyspaceSettings cloneFor(Class<?> type) {
			return new KeyspaceSettings(type, this.keyspace, false);
		}

		public String getKeyspace() {
			return keyspace;
		}

		public Class<?> getType() {
			return type;
		}

		public void setTimeToLive(Long timeToLive) {
			this.timeToLive = timeToLive;
		}

		public Long getTimeToLive() {
			return timeToLive;
		}

	}

	static class DefaultKeyspaceSetting extends KeyspaceSettings {

		public DefaultKeyspaceSetting(Class<?> type) {
			super(type, "#default#", false);
		}

	}

	public void addKeyspaceSettings(KeyspaceSettings keyspaceSettings) {

		Assert.notNull(keyspaceSettings);
		this.settingsMap.put(keyspaceSettings.getType(), keyspaceSettings);
	}
}
