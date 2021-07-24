package com.example;


import org.apache.kafka.connect.data.Struct;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;


@UdfDescription(name = "transform_haulage",
                author = "example user",
                version = "1.0.2",
                description = "Transforms shipment data")
public class TransformShipmentData  {


  
    @Udf(description = "The standard version of the formula with integer parameters.",schema="STRUCT<CITY STRUCT<NAME VARCHAR, STATUS VARCHAR, VALIDFROM VARCHAR, VALIDTO VARCHAR, LONGITUDE VARCHAR, LATITUDE VARCHAR, TIMEZONE VARCHAR, DAYLIGHTSAVINGTIME VARCHAR, DAYLIGHTSAVINGSTART VARCHAR, DAYLIGHTSAVINGSHIFTMINUTES VARCHAR, DESCRIPTION VARCHAR, WORKAROUNDREASON VARCHAR, PORTFLAG VARCHAR, OLSONTIMEZONE VARCHAR, ALTERNATENAMES STRUCT<ALTERNATENAME ARRAY<STRUCT<Name VARCHAR, description VARCHAR, status VARCHAR>>>, ALTERNATECODES STRUCT<ALTERNATECODE ARRAY<STRUCT<codeType VARCHAR, code VARCHAR>>>, COUNTRY STRUCT<name VARCHAR, ALTERNATECODES STRUCT<ALTERNATECODE ARRAY<STRUCT<codeType VARCHAR, code VARCHAR>>>>, PARENT STRUCT<name VARCHAR, type VARCHAR, ALTERNATECODES STRUCT<ALTERNATECODE ARRAY<STRUCT<codeType VARCHAR, code VARCHAR>>>>, BDA STRUCT<BDAYTYPE ARRAY<STRUCT<name VARCHAR, type VARCHAR, ALTERNATECODES STRUCT<ALTERNATECODE ARRAY<STRUCT<codeType VARCHAR, code VARCHAR>>>>>>>>")
    public  Struct trasformHaulage(@UdfParameter(schema = "struct <CITY STRUCT<NAME VARCHAR, STATUS VARCHAR, VALIDFROM VARCHAR, VALIDTO VARCHAR, LONGITUDE VARCHAR, LATITUDE VARCHAR, TIMEZONE VARCHAR, DAYLIGHTSAVINGTIME VARCHAR, DAYLIGHTSAVINGSTART VARCHAR, DAYLIGHTSAVINGSHIFTMINUTES VARCHAR, DESCRIPTION VARCHAR, WORKAROUNDREASON VARCHAR, PORTFLAG VARCHAR, OLSONTIMEZONE VARCHAR, ALTERNATENAMES STRUCT<ALTERNATENAME ARRAY<STRUCT<Name VARCHAR, description VARCHAR, status VARCHAR>>>, ALTERNATECODES STRUCT<ALTERNATECODE ARRAY<STRUCT<codeType VARCHAR, code VARCHAR>>>, COUNTRY STRUCT<name VARCHAR, ALTERNATECODES STRUCT<ALTERNATECODE ARRAY<STRUCT<codeType VARCHAR, code VARCHAR>>>>, PARENT STRUCT<name VARCHAR, type VARCHAR, ALTERNATECODES STRUCT<ALTERNATECODE ARRAY<STRUCT<codeType VARCHAR, code VARCHAR>>>>, BDA STRUCT<BDAYTYPE ARRAY<STRUCT<name VARCHAR, type VARCHAR, ALTERNATECODES STRUCT<ALTERNATECODE ARRAY<STRUCT<codeType VARCHAR, code VARCHAR>>>>>>>>") final Struct struct) {
    	
    	Struct city = (Struct)struct.get("city");
    	Struct  country = (Struct)city.get("country");
    	country.put("name", "My Country");
    	Struct  parent = (Struct)city.get("parent");
    	parent.put("name", "My Location");
    	return struct;
    }
}