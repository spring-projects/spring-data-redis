/*
 * Copyright (c) 2010 by J. Brisbin <jon@jbrisbin.com>
 * Portions (c) 2010 by NPC International, Inc. or the
 * original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.keyvalue.riak.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.data.keyvalue.riak.core.BucketKeyPair;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link MapReduceJob} for the Riak data store.
 *
 * @author J. Brisbin <jon@jbrisbin.com>
 */
@SuppressWarnings({"unchecked"})
public abstract class AbstractRiakMapReduceJob implements MapReduceJob {

	protected final Log log = LogFactory.getLog(getClass());
	protected List<Object> inputs = new LinkedList<Object>();
	protected List<MapReducePhase> phases = new ArrayList<MapReducePhase>();

	public List getInputs() {
		return this.inputs;
	}

	public MapReduceJob addInputs(List keys) {
		inputs.addAll(keys);
		return this;
	}

	public MapReduceJob addPhase(MapReducePhase phase) {
		phases.add(phase);
		return this;
	}

	public List<MapReducePhase> getPhases() {
		return this.phases;
	}

	public String toJson() {
		StringWriter out = new StringWriter();
		try {
			JsonGenerator json = new JsonFactory().createJsonGenerator(out);
			json.setCodec(new ObjectMapper());
			json.writeStartObject();

			// Inputs
			json.writeFieldName("inputs");
			if (1 == inputs.size() && !(inputs.get(0) instanceof List)) {
				json.writeString(inputs.get(0).toString());
			} else if (inputs.size() > 0) {
				json.writeStartArray();
				for (Object obj : inputs) {
					List pair = (List) obj;
					json.writeStartArray();
					json.writeString(pair.get(0).toString());
					json.writeString(pair.get(1).toString());
					json.writeEndArray();
				}
				json.writeEndArray();
			}

			// Query
			json.writeFieldName("query");
			json.writeStartArray();
			for (MapReducePhase phase : phases) {
				json.writeStartObject();
				switch (phase.getPhase()) {
					case MAP:
						json.writeFieldName("map");
						break;
					case REDUCE:
						json.writeFieldName("reduce");
						break;
					case LINK:
						json.writeFieldName("link");
				}

				json.writeStartObject();
				json.writeStringField("language", phase.getLanguage());
				Object repr = phase.getOperation().getRepresentation();
				if (repr instanceof String) {
					// Using source
					json.writeStringField("source",
																String.format("%s", phase.getOperation().getRepresentation()));
				} else if (repr instanceof BucketKeyPair) {
					BucketKeyPair pair = (BucketKeyPair) repr;
					json.writeStringField("bucket",
																String.format("%s", pair.getBucket()));
					json.writeStringField("key", String.format("%s", pair.getKey()));
				} else if (repr instanceof Map) {
					for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) repr).entrySet()) {
						json.writeStringField(entry.getKey().toString(),
																	entry.getValue().toString());
					}
				}
				if (phase.getKeepResults()) {
					json.writeBooleanField("keep", true);
				}
				// Arg
				if (null != phase.getArg()) {
					json.writeObjectField("arg", phase.getArg());
				}

				json.writeEndObject();
				json.writeEndObject();
			}
			json.writeEndArray();

			json.writeEndObject();
			json.flush();

		} catch (IOException e) {
			log.error(e.getMessage(), e);
		}
		return out.toString();
	}

}
