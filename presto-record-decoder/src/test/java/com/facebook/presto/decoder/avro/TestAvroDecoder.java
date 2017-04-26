/*
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
package com.facebook.presto.decoder.avro;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.DecoderTestColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.type.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.*;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.*;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

import static com.facebook.presto.decoder.util.DecoderTestUtil.checkValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestAvroDecoder {
    private static final AvroFieldDecoder DEFAULT_FIELD_DECODER = new AvroFieldDecoder();
    private final SchemaRegistryClient schemaRegistry;
    private final KafkaAvroSerializer avroSerializer;
    private final Map<String,String> dataMap;
    private final AvroRowDecoder avroDecoder;
    private final String topic;


    public TestAvroDecoder() {
        this.schemaRegistry = new MockSchemaRegistryClient();
        this.avroSerializer = new KafkaAvroSerializer(schemaRegistry);
        this.avroDecoder    = new AvroRowDecoder(schemaRegistry);
        this.dataMap        = new HashMap<>(1);
        this.topic          = "test";
    }


    private static Map<DecoderColumnHandle, FieldDecoder<?>> buildMap(List<DecoderColumnHandle> columns) {
        ImmutableMap.Builder<DecoderColumnHandle, FieldDecoder<?>> map = ImmutableMap.builder();
        for (DecoderColumnHandle column : columns) {
            map.put(column, DEFAULT_FIELD_DECODER);
        }
        return map.build();
    }

    @Test
    public void testSimple() throws IOException {

        Schema schema = SchemaBuilder.record("Foo").fields()
                .nullableBoolean("isBar", false)
                .name("toto").type().nullable().stringType().noDefault()
                .nullableInt("titiInt", 10)
                .endRecord();

        GenericData.Record sample = new GenericRecordBuilder(schema).set("isBar", true).set("toto", "toto").set("titiInt", 1234567).build();

        dataMap.put("SCHEMA", schema.toString(false));

        DecoderTestColumnHandle row1 = new DecoderTestColumnHandle("", 0, "row1", BooleanType.BOOLEAN, "0", null, null, false, false, false);
        DecoderTestColumnHandle row2 = new DecoderTestColumnHandle("", 1, "row2", createVarcharType(5), "1", null, null, false, false, false);
        DecoderTestColumnHandle row3 = new DecoderTestColumnHandle("", 2, "row3", IntegerType.INTEGER, "2", null, null, false, false, false);

        List<DecoderColumnHandle> columns = ImmutableList.of(row1, row2, row3);
        Set<FieldValueProvider> providers = new HashSet<>();

        boolean corrupt = avroDecoder.decodeRow(avroSerializer.serialize(topic, sample), dataMap, providers, columns, buildMap(columns));

        assertFalse(corrupt);
        assertEquals(providers.size(), columns.size());

        checkValue(providers, row1, true);
        checkValue(providers, row2, "toto");
        checkValue(providers, row3, 1234567);
    }

    @Test
    public void testBoolean() {

    }

    @Test
    public void testNulls() {

    }


}