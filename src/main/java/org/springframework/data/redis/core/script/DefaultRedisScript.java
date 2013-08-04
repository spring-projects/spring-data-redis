/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.redis.core.script;

import java.io.IOException;

import org.springframework.core.io.Resource;
import org.springframework.scripting.ScriptSource;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.scripting.support.StaticScriptSource;

/**
 * Default implementation of {@link RedisScript}. Delegates to an underlying {@link ScriptSource} to
 * retrieve script text and detect if script has been modified (and thus should have SHA1
 * re-calculated). This class is best used as a Singleton to avoid re-calculation of SHA1 on every
 * script execution.
 *
 * @author Jennifer Hickey
 *
 * @param <T>
 *            The script result type. Should be one of Long, Boolean, List, or deserialized value
 *            type. Can be null if the script returns a throw-away status (i.e "OK")
 */
public class DefaultRedisScript<T> implements RedisScript<T> {

	private ScriptSource scriptSource;

	private String sha1;

	private Class<T> resultType;

	private final Object shaModifiedMonitor = new Object();

	/**
	 *
	 * @param scriptLocation
	 *            The location of the script
	 * @param resultType
	 *            The Script result type
	 */
	public DefaultRedisScript(Resource scriptLocation, Class<T> resultType) {
		this(new ResourceScriptSource(scriptLocation), resultType);
	}

	/**
	 *
	 * @param script
	 *            The script
	 * @param resultType
	 *            The Script result type
	 */
	public DefaultRedisScript(String script, Class<T> resultType) {
		this(new StaticScriptSource(script), resultType);
	}

	/**
	 *
	 * @param scriptSource
	 *            The {@link ScriptSource} of the script
	 * @param resultType
	 *            The Script result type
	 */
	public DefaultRedisScript(ScriptSource scriptSource, Class<T> resultType) {
		this.scriptSource = scriptSource;
		this.resultType = resultType;
	}

	public String getSha1() {
		synchronized (shaModifiedMonitor) {
			if (sha1 == null || scriptSource.isModified()) {
				this.sha1 = DigestUtils.sha1DigestAsHex(getScriptAsString());
			}
			return sha1;
		}
	}

	public Class<T> getResultType() {
		return this.resultType;
	}

	public String getScriptAsString() {
		try {
			return scriptSource.getScriptAsString();
		} catch (IOException e) {
			throw new ScriptingException("Error reading script text", e);
		}
	}
}
