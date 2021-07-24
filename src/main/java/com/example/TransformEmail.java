package com.example;


import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;


@UdfDescription(name = "transform_email",
                author = "example user",
                version = "1.0.2",
                description = "Transforms shipment data")
public class TransformEmail  {


	@Udf(description = "do stuff", schema="STRUCT<codeType VARCHAR(STRING),code VARCHAR(STRING)>")
    public Struct MyCustomUdf(@UdfParameter(schema = "struct <codeType VARCHAR(STRING), code VARCHAR(STRING)>", value = "user") final Struct struct) {
		struct.put("code", "my-code");
		struct.put("codeType", "my-codetype");
      return struct;
    }

}