package com.facebook.presto.decoder.avro;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import io.airlift.slice.Slice;

import java.util.Set;

import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class AvroFieldDecoder implements FieldDecoder<Object> {
    @Override
    public Set<Class<?>> getJavaTypes() {
        return null;
    }

    @Override
    public String getRowDecoderName() {
        return null;
    }

    @Override
    public String getFieldDecoderName() {
        return null;
    }

    @Override
    public FieldValueProvider decode(Object value, DecoderColumnHandle columnHandle) {
        requireNonNull(columnHandle, "columnHandle is null");

        return new FieldValueProvider()
        {
            @Override
            public boolean accept(DecoderColumnHandle handle)
            {
                return columnHandle.equals(handle);
            }

            @Override
            public boolean isNull()
            {
                return (value == null);
            }

            @SuppressWarnings("SimplifiableConditionalExpression")
            @Override
            public boolean getBoolean()
            {
                return isNull() ? false : (boolean) value;
            }

            @Override
            public long getLong()
            {
                return isNull() ? 0L : Integer.toUnsignedLong((Integer) value);
            }

            @Override
            public double getDouble()
            {
                return isNull() ? 0.0d : (double) value;
            }

            @Override
            public Slice getSlice()
            {
                if (isNull()) {
                    return EMPTY_SLICE;
                }
                Slice slice = utf8Slice(value.toString());
                if (isVarcharType(columnHandle.getType())) {
                    slice = truncateToLength(slice, columnHandle.getType());
                }
                return slice;
            }
        };
    }
}
