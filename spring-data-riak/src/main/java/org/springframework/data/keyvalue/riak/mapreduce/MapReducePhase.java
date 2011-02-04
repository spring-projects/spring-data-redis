/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 *     Portions (c) 2010 by NPC International, Inc. or the
 *     original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.keyvalue.riak.mapreduce;

/**
 * A generic interface to the phases of Map/Reduce jobs.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
public interface MapReducePhase {

	public static enum Phase {
		MAP, REDUCE, LINK
	}

	/**
	 * The bucket pattern to match on link phases.
	 *
	 * @return
	 */
	String getBucket();

	/**
	 * Set the bucket pattern to match on link phases.
	 *
	 * @param bucket
	 */
	void setBucket(String bucket);

	Phase getPhase();

	/**
	 * The language this phase is described in.
	 *
	 * @return
	 */
	String getLanguage();

	/**
	 * Whether or not to keep the result of this phase.
	 *
	 * @return
	 */
	boolean getKeepResults();

	/**
	 * Get the operation this phase will execute.
	 *
	 * @return
	 */
	MapReduceOperation getOperation();

	/**
	 * Set the static argument for this job.
	 *
	 * @param arg
	 */
	void setArg(Object arg);

	/**
	 * Get the static argument for this phase.
	 *
	 * @return
	 */
	Object getArg();
}
