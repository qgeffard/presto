package com.facebook.presto.decoder.avro;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class AvroRowDecoder implements RowDecoder {
    public static final String NAME = "avro";
    public static final String SCHEMA = "SCHEMA";
    private Schema.Parser parser = new Schema.Parser();
    private SchemaRegistryClient schemaRegistryClient;
    private KafkaAvroDeserializer deserializer;

    public AvroRowDecoder(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
        this.deserializer = new KafkaAvroDeserializer(this.schemaRegistryClient);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Config("kafka.schema.registry.url") //TODO check @config fonctionnement ???
    public AvroRowDecoder setSchemaRegistryUrl(String schemaRegistryUrl){
        this.schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl,Integer.MAX_VALUE);
        return this;
    }


    @Override
    public boolean decodeRow(byte[] data, Map<String, String> dataMap, Set<FieldValueProvider> fieldValueProviders, List<DecoderColumnHandle> columnHandles, Map<DecoderColumnHandle, FieldDecoder<?>> fieldDecoders) {
        Schema schema = parser.parse(dataMap.get(SCHEMA)); //TODO check how to pass schema in dataMap and check if it 's realy needed ???
        Object obj = deserializer.deserialize(null, data, schema);

        if(!GenericData.Record.class.isInstance(obj))
            throw new RuntimeException("Fail to deserialize");

        GenericData.Record record = (GenericData.Record) obj;


        for (DecoderColumnHandle columnHandle: columnHandles) {

            if (columnHandle.isInternal()) {
                continue;
            }

            AvroFieldDecoder decoder = (AvroFieldDecoder) fieldDecoders.get(columnHandle);

            fieldValueProviders.add(decoder.decode(record.get(columnHandle.getName()), columnHandle));
        }

        return false;
    }
}
