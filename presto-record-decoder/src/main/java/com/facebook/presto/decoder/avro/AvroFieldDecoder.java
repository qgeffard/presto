package com.facebook.presto.decoder.avro;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;

import java.util.Set;

public class AvroFieldDecoder implements FieldDecoder<String> {
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
    public FieldValueProvider decode(String value, DecoderColumnHandle columnHandle) {
        return null;
    }
}
