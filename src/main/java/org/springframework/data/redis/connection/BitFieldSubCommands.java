/*
 * Copyright 2018-2023 the original author or authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.data.redis.connection.BitFieldSubCommands.BitFieldSubCommand;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * The actual {@code BITFIELD} command representation holding several {@link BitFieldSubCommand}s to execute.
 *
 * @author Christoph Strobl
 * @author Qiang Lee
 * @author Yanam
 * @since 2.1
 */
public class BitFieldSubCommands implements Iterable<BitFieldSubCommand> {

	private final List<BitFieldSubCommand> subCommands;

	private BitFieldSubCommands(List<BitFieldSubCommand> subCommands) {
		this.subCommands = new ArrayList<>(subCommands);
	}

	private BitFieldSubCommands(List<BitFieldSubCommand> subCommands, BitFieldSubCommand subCommand) {

		this(subCommands);

		Assert.notNull(subCommand, "SubCommand must not be null!");
		this.subCommands.add(subCommand);
	}

	/**
	 * Creates a new {@link BitFieldSubCommands}.
	 *
	 * @return
	 */
	public static BitFieldSubCommands create() {
		return new BitFieldSubCommands(Collections.emptyList());
	}

	/**
	 * Creates a new {@link BitFieldSubCommands} with Multiple BitFieldSubCommand.
	 *
	 * @return
	 * @since 2.5.2
	 */
	public static BitFieldSubCommands create(BitFieldSubCommand... subCommands) {

		Assert.notNull(subCommands, "Subcommands must not be null");

		return new BitFieldSubCommands(Arrays.asList(subCommands));
	}

	/**
	 * Obtain a new {@link BitFieldGetBuilder} for creating and adding a {@link BitFieldGet} sub command.
	 *
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public BitFieldGetBuilder get(BitFieldType type) {
		return new BitFieldGetBuilder(this).forType(type);
	}

	/**
	 * Create new {@link BitFieldSubCommands} adding given {@link BitFieldGet} to the sub commands.
	 *
	 * @param get must not be {@literal null}.
	 * @return
	 */
	protected BitFieldSubCommands get(BitFieldGet get) {
		return new BitFieldSubCommands(subCommands, get);
	}

	/**
	 * Obtain a new {@link BitFieldSetBuilder} for creating and adding a {@link BitFieldSet} sub command.
	 *
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public BitFieldSetBuilder set(BitFieldType type) {
		return new BitFieldSetBuilder(this).forType(type);
	}

	/**
	 * Create new {@link BitFieldSubCommands} adding given {@link BitFieldSet} to the sub commands.
	 *
	 * @param set must not be {@literal null}.
	 * @return
	 */
	protected BitFieldSubCommands set(BitFieldSet set) {
		return new BitFieldSubCommands(subCommands, set);
	}

	/**
	 * Obtain a new {@link BitFieldIncrByBuilder} for creating and adding a {@link BitFieldIncrBy} sub command.
	 *
	 * @param type must not be {@literal null}.
	 * @return
	 */
	public BitFieldIncrByBuilder incr(BitFieldType type) {
		return new BitFieldIncrByBuilder(this).forType(type);
	}

	/**
	 * Create new {@link BitFieldSubCommands} adding given {@link BitFieldIncrBy} to the sub commands.
	 *
	 * @param incrBy must not be {@literal null}.
	 * @return
	 */
	protected BitFieldSubCommands incr(BitFieldIncrBy incrBy) {
		return new BitFieldSubCommands(subCommands, incrBy);
	}

	/**
	 * Get the {@link List} of sub commands.
	 *
	 * @return never {@literal null}.
	 */
	public List<BitFieldSubCommand> getSubCommands() {
		return subCommands;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<BitFieldSubCommand> iterator() {
		return subCommands.iterator();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof BitFieldSubCommands)) {
			return false;
		}
		BitFieldSubCommands that = (BitFieldSubCommands) o;
		return ObjectUtils.nullSafeEquals(subCommands, that.subCommands);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(subCommands);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		final StringBuffer sb = new StringBuffer();
		sb.append(getClass().getSimpleName());
		sb.append(" [subCommands=").append(subCommands);
		sb.append(']');
		return sb.toString();
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class BitFieldSetBuilder {

		private BitFieldSubCommands ref;

		BitFieldSet set = new BitFieldSet();

		private BitFieldSetBuilder(BitFieldSubCommands ref) {
			this.ref = ref;
		}

		public BitFieldSetBuilder forType(BitFieldType type) {
			this.set.type = type;
			return this;
		}

		/**
		 * Set the zero based bit {@literal offset}.
		 *
		 * @param offset must not be {@literal null}.
		 * @return
		 */
		public BitFieldSetBuilder valueAt(long offset) {
			return valueAt(Offset.offset(offset));
		}

		/**
		 * Set the bit offset.
		 *
		 * @param offset must not be {@literal null}.
		 * @return
		 */
		public BitFieldSetBuilder valueAt(Offset offset) {

			Assert.notNull(offset, "Offset must not be null!");

			this.set.offset = offset;
			return this;
		}

		/**
		 * Set the value.
		 *
		 * @param value must not be {@literal null}.
		 * @return
		 */
		public BitFieldSubCommands to(long value) {

			this.set.value = value;
			return ref.set(this.set);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public static class BitFieldGetBuilder {

		private BitFieldSubCommands ref;

		BitFieldGet get = new BitFieldGet();

		private BitFieldGetBuilder(BitFieldSubCommands ref) {
			this.ref = ref;
		}

		public BitFieldGetBuilder forType(BitFieldType type) {
			this.get.type = type;
			return this;
		}

		/**
		 * Set the zero based bit {@literal offset}.
		 *
		 * @param offset must not be {@literal null}.
		 * @return
		 */
		public BitFieldSubCommands valueAt(long offset) {
			return valueAt(Offset.offset(offset));
		}

		/**
		 * Set the bit offset.
		 *
		 * @param offset must not be {@literal null}.
		 * @return
		 */
		public BitFieldSubCommands valueAt(Offset offset) {

			Assert.notNull(offset, "Offset must not be null!");

			this.get.offset = offset;
			return ref.get(this.get);
		}
	}

	/**
	 * @author Christoph Strobl
	 */
	public class BitFieldIncrByBuilder {

		private BitFieldSubCommands ref;

		BitFieldIncrBy incrBy = new BitFieldIncrBy();

		private BitFieldIncrByBuilder(BitFieldSubCommands ref) {
			this.ref = ref;
		}

		public BitFieldIncrByBuilder forType(BitFieldType type) {
			this.incrBy.type = type;
			return this;
		}

		/**
		 * Set the zero based bit {@literal offset}.
		 *
		 * @param offset must not be {@literal null}.
		 * @return
		 */
		public BitFieldIncrByBuilder valueAt(long offset) {
			return valueAt(Offset.offset(offset));
		}

		/**
		 * Set the bit offset.
		 *
		 * @param offset must not be {@literal null}.
		 * @return
		 */
		public BitFieldIncrByBuilder valueAt(Offset offset) {

			Assert.notNull(offset, "Offset must not be null!");
			this.incrBy.offset = offset;
			return this;
		}

		/**
		 * Set the {@link BitFieldIncrBy.Overflow} to be used for this and any subsequent {@link BitFieldIncrBy} commands.
		 *
		 * @param overflow
		 * @return
		 */
		public BitFieldIncrByBuilder overflow(BitFieldIncrBy.Overflow overflow) {
			this.incrBy.overflow = overflow;
			return this;
		}

		/**
		 * Set the value used for increasing.
		 *
		 * @param value
		 * @return
		 */
		public BitFieldSubCommands by(long value) {

			this.incrBy.value = value;
			return ref.incr(this.incrBy);
		}
	}

	/**
	 * Sub command to be used as part of {@link BitFieldSubCommands}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public interface BitFieldSubCommand {

		/**
		 * The actual sub command
		 *
		 * @return never {@literal null}.
		 */
		String getCommand();

		/**
		 * The {@link BitFieldType} to apply for the command.
		 *
		 * @return never {@literal null}.
		 */
		BitFieldType getType();

		/**
		 * The bit offset to apply for the command.
		 *
		 * @return never {@literal null}.
		 */
		Offset getOffset();
	}

	/**
	 * Offset used inside a {@link BitFieldSubCommand}. Can be zero or type based. See
	 * <a href="https://redis.io/commands/bitfield#bits-and-positional-offsets">Bits and positional offsets</a> in the
	 * Redis reference.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.1
	 */
	public static class Offset {

		private final long offset;
		private final boolean zeroBased;

		private Offset(long offset, boolean zeroBased) {

			this.offset = offset;
			this.zeroBased = zeroBased;
		}

		/**
		 * Creates new zero based offset. <br />
		 * <b>NOTE:</b> change to type based offset by calling {@link #multipliedByTypeLength()}.
		 *
		 * @param offset must not be {@literal null}.
		 * @return
		 */
		public static Offset offset(long offset) {
			return new Offset(offset, true);
		}

		/**
		 * Creates new type based offset.
		 *
		 * @return
		 */
		public Offset multipliedByTypeLength() {
			return new Offset(offset, false);
		}

		/**
		 * @return true if offset starts at 0 and is not multiplied by the type length.
		 */
		public boolean isZeroBased() {
			return zeroBased;
		}

		/**
		 * @return the actual offset value
		 */
		public long getValue() {
			return offset;
		}

		/**
		 * @return the Redis Command representation
		 */
		public String asString() {
			return (isZeroBased() ? "" : "#") + getValue();
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return asString();
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(@Nullable Object o) {

			if (this == o) {
				return true;
			}
			if (!(o instanceof Offset)) {
				return false;
			}
			Offset that = (Offset) o;
			if (offset != that.offset) {
				return false;
			}
			return zeroBased == that.zeroBased;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			int result = (int) (offset ^ (offset >>> 32));
			result = 31 * result + (zeroBased ? 1 : 0);
			return result;
		}
	}

	/**
	 * The actual Redis bitfield type representation for signed and unsigned integers used with
	 * {@link BitFieldSubCommand}.
	 *
	 * @author Christoph Strobl
	 * @author Mark Paluch
	 * @since 2.1
	 */
	public static class BitFieldType {

		/** 8 bit signed Integer */
		public static final BitFieldType INT_8 = new BitFieldType(true, 8);

		/** 16 bit signed Integer */
		public static final BitFieldType INT_16 = new BitFieldType(true, 16);

		/** 32 bit signed Integer */
		public static final BitFieldType INT_32 = new BitFieldType(true, 32);

		/** 64 bit signed Integer */
		public static final BitFieldType INT_64 = new BitFieldType(true, 64);

		/** 8 bit unsigned Integer */
		public static final BitFieldType UINT_8 = new BitFieldType(false, 8);

		/** 16 bit unsigned Integer */
		public static final BitFieldType UINT_16 = new BitFieldType(false, 16);

		/** 32 bit unsigned Integer */
		public static final BitFieldType UINT_32 = new BitFieldType(false, 32);

		/** 64 bit unsigned Integer */
		public static final BitFieldType UINT_64 = new BitFieldType(false, 64);

		private final boolean signed;
		private final int bits;

		private BitFieldType(boolean signed, Integer bits) {

			this.signed = signed;
			this.bits = bits;
		}

		/**
		 * Create new signed {@link BitFieldType}.
		 *
		 * @param bits must not be {@literal null}.
		 * @return
		 */
		public static BitFieldType signed(int bits) {
			return new BitFieldType(true, bits);
		}

		/**
		 * Create new unsigned {@link BitFieldType}.
		 *
		 * @param bits must not be {@literal null}.
		 * @return
		 */
		public static BitFieldType unsigned(int bits) {
			return new BitFieldType(false, bits);
		}

		/**
		 * @return true if {@link BitFieldType} is signed.
		 */
		public boolean isSigned() {
			return signed;
		}

		/**
		 * Get the actual bits of the type.
		 *
		 * @return never {@literal null}.
		 */
		public int getBits() {
			return bits;
		}

		/**
		 * Get the Redis Command representation.
		 *
		 * @return
		 */
		public String asString() {
			return (isSigned() ? "i" : "u") + getBits();
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(@Nullable Object o) {

			if (this == o) {
				return true;
			}
			if (!(o instanceof BitFieldType)) {
				return false;
			}
			BitFieldType that = (BitFieldType) o;
			if (signed != that.signed) {
				return false;
			}
			return bits == that.bits;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			int result = (signed ? 1 : 0);
			result = 31 * result + bits;
			return result;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return asString();
		}

	}

	/**
	 * @author Christoph Strobl
	 */
	public static abstract class AbstractBitFieldSubCommand implements BitFieldSubCommand {

		BitFieldType type;
		Offset offset;

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitFieldSubCommand#getType()
		 */
		@Override
		public BitFieldType getType() {
			return type;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitFieldSubCommand#getOffset()
		 */
		@Override
		public Offset getOffset() {
			return offset;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(@Nullable Object o) {

			if (this == o) {
				return true;
			}
			if (!(o instanceof AbstractBitFieldSubCommand)) {
				return false;
			}
			AbstractBitFieldSubCommand that = (AbstractBitFieldSubCommand) o;
			if (!ObjectUtils.nullSafeEquals(getClass(), that.getClass())) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(type, that.type)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(offset, that.offset);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {

			int result = ObjectUtils.nullSafeHashCode(type);
			result = 31 * result + ObjectUtils.nullSafeHashCode(offset);
			return result;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [type=").append(type);
			sb.append(", offset=").append(offset);
			sb.append(']');
			return sb.toString();
		}
	}

	/**
	 * The {@code SET} sub command used with {@link BitFieldSubCommands}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class BitFieldSet extends AbstractBitFieldSubCommand {

		private long value;

		/**
		 * Creates a new {@link BitFieldSet}.
		 *
		 * @param type must not be {@literal null}.
		 * @param offset must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @return
		 * @since 2.5.2
		 */
		public static BitFieldSet create(BitFieldType type,Offset offset,long value){

			Assert.notNull(type, "BitFieldType must not be null");
			Assert.notNull(offset, "Offset must not be null");

			BitFieldSet instance = new BitFieldSet();
			instance.type =  type;
			instance.offset = offset;
			instance.value = value;

			return instance;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitFieldSubCommand#getCommand()
		 */
		@Override
		public String getCommand() {
			return "SET";
		}

		/**
		 * Get the value to set.
		 *
		 * @return never {@literal null}.
		 */
		public long getValue() {
			return value;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof BitFieldSet)) {
				return false;
			}
			if (!super.equals(o)) {
				return false;
			}
			BitFieldSet that = (BitFieldSet) o;
			if (value != that.value) {
				return false;
			}
			return true;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			int result = super.hashCode();
			result = 31 * result + (int) (value ^ (value >>> 32));
			return result;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [type=").append(type);
			sb.append(", offset=").append(offset);
			sb.append(", value=").append(value);
			sb.append(']');
			return sb.toString();
		}
	}

	/**
	 * The {@code GET} sub command used with {@link BitFieldSubCommands}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class BitFieldGet extends AbstractBitFieldSubCommand {

		/**
		 * Creates a new {@link BitFieldGet}.
		 *
		 * @param type must not be {@literal null}.
		 * @param offset must not be {@literal null}.
		 * @since 2.5.2
		 * @return
		 */
		public static BitFieldGet create(BitFieldType type,Offset offset){

			Assert.notNull(type, "BitFieldType must not be null");
			Assert.notNull(offset, "Offset must not be null");

			BitFieldGet instance = new BitFieldGet();
			instance.type =  type;
			instance.offset = offset;

			return instance;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitFieldSubCommand#getCommand()
		 */
		@Override
		public String getCommand() {
			return "GET";
		}

	}

	/**
	 * The {@code INCRBY} sub command used with {@link BitFieldSubCommands}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class BitFieldIncrBy extends AbstractBitFieldSubCommand {

		private long value;
		private @Nullable Overflow overflow;

		/**
		 * Creates a new {@link BitFieldIncrBy}.
		 *
		 * @param type must not be {@literal null}.
		 * @param offset must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @return
		 * @since 2.5.2
		 */
		public static BitFieldIncrBy create(BitFieldType type,Offset offset,long value){
			return create(type, offset, value, null);
		}

		/**
		 * Creates a new {@link BitFieldIncrBy}.
		 *
		 * @param type must not be {@literal null}.
		 * @param offset must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @param overflow can be {@literal null} to use redis defaults.
		 * @since 2.5.2
		 * @return
		 */
		public static BitFieldIncrBy create(BitFieldType type, Offset offset, long value, @Nullable Overflow overflow) {

			Assert.notNull(type, "BitFieldType must not be null");
			Assert.notNull(offset, "Offset must not be null");

			BitFieldIncrBy instance = new BitFieldIncrBy();
			instance.type =  type;
			instance.offset = offset;
			instance.value = value;
			instance.overflow = overflow;

			return instance;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.redis.connection.StringRedisConnection.BitFieldSubCommand#getCommand()
		 */
		@Override
		public String getCommand() {
			return "INCRBY";
		}

		/**
		 * Get the increment value.
		 *
		 * @return never {@literal null}.
		 */
		public long getValue() {
			return value;
		}

		/**
		 * Get the overflow to apply. Can be {@literal null} to use redis defaults.
		 *
		 * @return can be {@literal null}.
		 */
		@Nullable
		public Overflow getOverflow() {
			return overflow;
		}

		/**
		 * @author Christoph Strobl
		 */
		public enum Overflow {
			SAT, FAIL, WRAP
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(@Nullable Object o) {

			if (this == o) {
				return true;
			}
			if (!(o instanceof BitFieldIncrBy)) {
				return false;
			}
			BitFieldIncrBy that = (BitFieldIncrBy) o;
			if (value != that.value) {
				return false;
			}
			return overflow == that.overflow;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {

			int result = (int) (value ^ (value >>> 32));
			result = 31 * result + ObjectUtils.nullSafeHashCode(overflow);
			return result;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append(getClass().getSimpleName());
			sb.append(" [type=").append(type);
			sb.append(", offset=").append(offset);
			sb.append(", value=").append(value);
			sb.append(", overflow=").append(overflow);
			sb.append(']');
			return sb.toString();
		}
	}
}
