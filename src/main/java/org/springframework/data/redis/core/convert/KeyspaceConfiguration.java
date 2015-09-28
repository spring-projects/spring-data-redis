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

import org.springframework.util.ClassUtils;

/**
 * @author Christoph Strobl
 */
public class KeyspaceConfiguration {

	private Map<Class<?>, KeyspaceAssignment> assignmentMap;

	public KeyspaceConfiguration() {

		this.assignmentMap = new ConcurrentHashMap<Class<?>, KeyspaceAssignment>();
		for (KeyspaceAssignment initial : initialConfiguration()) {
			assignmentMap.put(initial.type, initial);
		}
	}

	public boolean hasAssignmentFor(Class<?> type) {

		if (assignmentMap.containsKey(type)) {

			if (assignmentMap.get(type) instanceof DefaultKeyspaceAssignment) {
				return false;
			}

			return true;
		}

		for (KeyspaceAssignment assignment : assignmentMap.values()) {
			if (assignment.inherit) {
				if (ClassUtils.isAssignable(assignment.type, type)) {
					assignmentMap.put(type, assignment.cloneFor(type));
					return true;
				}
			}
		}

		assignmentMap.put(type, new DefaultKeyspaceAssignment(type));
		return false;
	}

	public KeyspaceAssignment getKeyspaceAssignment(Class<?> type) {

		KeyspaceAssignment assignment = assignmentMap.get(type);
		if (assignment == null || assignment instanceof DefaultKeyspaceAssignment) {
			return null;
		}

		return assignment;
	}

	/**
	 * Customization hook.
	 * 
	 * @return must not return {@literal null}.
	 */
	protected Iterable<KeyspaceAssignment> initialConfiguration() {
		return Collections.emptySet();
	}

	public static class KeyspaceAssignment {

		private final String keyspace;
		private final Class<?> type;
		private final boolean inherit;

		public KeyspaceAssignment(Class<?> type, String keyspace) {
			this(type, keyspace, true);
		}

		public KeyspaceAssignment(Class<?> type, String keyspace, boolean inherit) {

			this.type = type;
			this.keyspace = keyspace;
			this.inherit = inherit;
		}

		KeyspaceAssignment cloneFor(Class<?> type) {
			return new KeyspaceAssignment(type, this.keyspace, false);
		}

		public String getKeyspace() {
			return keyspace;
		}

		public Class<?> getType() {
			return type;
		}

	}

	static class DefaultKeyspaceAssignment extends KeyspaceAssignment {

		public DefaultKeyspaceAssignment(Class<?> type) {
			super(type, "#default#", false);
		}

	}
}
