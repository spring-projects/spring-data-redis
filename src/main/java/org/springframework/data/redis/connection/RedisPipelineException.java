/*
 * Copyright 2011-2018 the original author or authors.
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

import java.util.Collections;
import java.util.List;

import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.lang.Nullable;

/**
 * Exception thrown when executing/closing a pipeline that contains one or multiple invalid/incorrect statements. The
 * exception might also contain the pipeline result (if the driver returns it), allowing for analysis and tracing.
 * <p>
 * Typically, the first exception returned by the pipeline is used as the <i>cause</i> of this exception for easier
 * debugging.
 *
 * @author Costin Leau
 */
public class RedisPipelineException extends InvalidDataAccessResourceUsageException {

	private final List<Object> results;

	/**
	 * Constructs a new <code>RedisPipelineException</code> instance.
	 *
	 * @param msg the message
	 * @param cause the cause
	 * @param pipelineResult the pipeline result
	 */
	public RedisPipelineException(@Nullable String msg, @Nullable Throwable cause, List<Object> pipelineResult) {
		super(msg, cause);
		results = Collections.unmodifiableList(pipelineResult);
	}

	/**
	 * Constructs a new <code>RedisPipelineException</code> instance using a default message.
	 *
	 * @param cause the cause
	 * @param pipelineResult the pipeline result
	 */
	public RedisPipelineException(Exception cause, List<Object> pipelineResult) {
		this("Pipeline contained one or more invalid commands", cause, pipelineResult);
	}

	/**
	 * Constructs a new <code>RedisPipelineException</code> instance using a default message and an empty pipeline result
	 * list.
	 *
	 * @param cause the cause
	 */
	public RedisPipelineException(Exception cause) {
		this("Pipeline contained one or more invalid commands", cause, Collections.emptyList());
	}

	/**
	 * Constructs a new <code>RedisPipelineException</code> instance.
	 *
	 * @param msg message
	 * @param pipelineResult pipeline partial results
	 */
	public RedisPipelineException(String msg, List<Object> pipelineResult) {
		super(msg);
		results = Collections.unmodifiableList(pipelineResult);
	}

	/**
	 * Optionally returns the result of the pipeline that caused the exception. Typically contains both the results of the
	 * successful statements but also the exceptions of the incorrect ones.
	 *
	 * @return result of the pipeline
	 */
	public List<Object> getPipelineResult() {
		return results;
	}
}
