/*
 * Copyright 2014 the original author or authors.
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


/**
 * ZRangeOptions is to provide user add options to zRange related methods programmatically. like this new
 * ZRangeOptions().score().greaterThanInclusive(1.1)
 * lessThanInclusive(4.0).withScores().limitedTo().offset(1).count(1).build());
 * 
 * @author David Liu
 * @since 1.2
 *
 */
public class ZRangeOptions {

	private boolean score = false;
	private Object min;
	private Object max;
	private boolean inclusiveMax = false;
	private boolean inclusiveMin = false;
	private boolean limited = false;
	private boolean withScores = false;
	private boolean rev = false;
	private List<Object> valueList = new ArrayList<Object>();
	private List<Class> typeList = new ArrayList<Class>();
	private List<Param> paramList = new ArrayList<Param>();
	public static final int STARTINDEX = 1;
	public static final int ENDINDEX = 2;
	public static final int OFFSETINDEX = 3;
	public static final int COUNTINDEX = 4;
	private enum Type{
		DOUBLE,STRING
	}
	private Type type;

	public ZRangeOptions start(long value) {
		paramList.add(new Param(STARTINDEX, value, long.class));
		return this;
	}

	public ZRangeOptions end(long value) {
		paramList.add(new Param(ENDINDEX, value, long.class));
		return this;
	}

	private class Param {
		private int index;
		private Object value;
		private Class type;

		public Param(int index, Object value, Class type) {
			this.index = index;
			this.value = value;
			this.type = type;
		}
		/**
		 * @return the index
		 */
		public int getIndex() {
			return index;
		}
		/**
		 * @param index the index to set
		 */
		public void setIndex(int index) {
			this.index = index;
		}
		/**
		 * @return the value
		 */
		public Object getValue() {
			return value;
		}
		/**
		 * @param value the value to set
		 */
		public void setValue(Object value) {
			this.value = value;
		}
		/**
		 * @return the type
		 */
		public Class getType() {
			return type;
		}
		/**
		 * @param type the type to set
		 */
		public void setType(Class type) {
			this.type = type;
		}
		
	}


	public ZRangeOptions withScores() {
		this.withScores = true;
		return this;
	}

	public ZRangeOptions rev() {
		this.rev = true;
		return this;
	}

	public ZRangeOptions limitedTo() {
		this.limited = true;
		return this;
	}

	public ZRangeOptions offset(long value) {
		if (this.limited) {
			paramList.add(new Param(OFFSETINDEX, value, long.class));
		}
		else {
			throw new IllegalStateException("limited is not set");
		}
		return this;
	}

	public ZRangeOptions count(long value) {
		if (this.limited) {
			paramList.add(new Param(COUNTINDEX, value, long.class));
		}
		else {
			throw new IllegalStateException("limited is not set");
		}
		return this;
	}

	public ZRangeOptions score() {
		this.score = true;
		return this;
	}

	public ZRangeOptions lessThanInclusive(double value) {
		if(this.type == null) {
			this.type = Type.DOUBLE;
		}
		this.max = value;
		this.inclusiveMax = true;
		return this;
	}

	public ZRangeOptions lessThanExclusive(double value) {
		this.type = Type.STRING;
		this.max = value;
		return this;
	}

	public ZRangeOptions greaterThanInclusive(double value) {
		if(this.type == null) {
			this.type = Type.DOUBLE;
		}
		this.min = value;
		this.inclusiveMin = true;
		return this;
	}

	public ZRangeOptions greaterThanExclusive(double value) {
		this.type = Type.STRING;
		this.min = value;
		return this;
	}

	public ZRangeOptions greaterThanInfinite() {
		this.paramList.add(new Param(STARTINDEX, Double.MIN_VALUE, double.class));
		return this;
	}

	public ZRangeOptions lessThanInfinite() {
		this.paramList.add(new Param(ENDINDEX, Double.MAX_VALUE, double.class));
		return this;
	}

	public TargetMethod build() {
		StringBuilder sb = new StringBuilder();
		if (this.rev) {
			sb.append("zRevRange");
		} else {
			sb.append("zRange");
		}
		if (this.score) {
			sb.append("ByScore");
		}
		if (this.withScores) {
			sb.append("WithScores");
		}

		generateParamAndValueList();
		return new TargetMethod(sb.toString(), this.typeList, this.valueList);
	}
	
	public class ComparatorParam implements Comparator{

		 public int compare(Object arg0, Object arg1) {
			 Param user0 = (Param)arg0;
			 Param user1 = (Param)arg1;
		  return user0.getIndex()-user1.getIndex();
		 }
	}

	@SuppressWarnings("unchecked")
	private void generateParamAndValueList() {
		this.typeList.add(byte[].class);
		if (this.type != null) {
			if (this.type.equals(Type.DOUBLE)) {
				this.paramList.add(new Param(ENDINDEX, this.max, double.class));
				this.paramList.add(new Param(STARTINDEX, this.min, double.class));
			}
			else if (this.type.equals(Type.STRING)) {
				if (this.inclusiveMax) {
					this.paramList.add(new Param(ENDINDEX, this.max.toString(), String.class));
				}
				else {
					this.paramList.add(new Param(ENDINDEX, "(" + this.max, String.class));
				}
				if (this.inclusiveMin) {
					this.paramList.add(new Param(STARTINDEX, this.min.toString(), String.class));
				}
				else {
					this.paramList.add(new Param(STARTINDEX, "(" + this.min, String.class));
				}
			}
		}
		ComparatorParam comparator=new ComparatorParam();
		Collections.sort(this.paramList,comparator);
		for (Param param : this.paramList) {
			this.typeList.add(param.getType());
			this.valueList.add(param.getValue());
		}
	}

}
