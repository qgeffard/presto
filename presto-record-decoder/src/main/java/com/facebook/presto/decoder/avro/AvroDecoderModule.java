package com.facebook.presto.decoder.avro;


import com.google.inject.Binder;
import com.google.inject.Module;

import static com.facebook.presto.decoder.DecoderModule.bindFieldDecoder;
import static com.facebook.presto.decoder.DecoderModule.bindRowDecoder;

public class AvroDecoderModule implements Module{

    @Override
    public void configure(Binder binder) {
        bindRowDecoder(binder, AvroRowDecoder.class);

        bindFieldDecoder(binder, AvroFieldDecoder.class);
    }
}
