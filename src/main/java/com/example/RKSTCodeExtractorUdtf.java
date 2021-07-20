package com.example;

import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.connect.data.Struct;
import java.util.ArrayList;
import java.util.List;
import com.example.*;

@UdtfDescription(name = "get_rkst",
                 author = "example user",
                 version = "1.5.0",
                 description = "Disassembles a sequence and produces new elements concatenated with indices.")
public class RKSTCodeExtractorUdtf {

    private final String DELIMITER = "-";

    @Udtf(description = "Takes an array of any type and returns rows with each element paired to its index.")
    public <E> List<String> indexSequence(@UdfParameter List<E> x) {
        List<String> result = new ArrayList<>();

        for(int i = 0; i < x.size(); i++) { 
        	if(((Struct)x.get(i)).getString("codeType").equals("RKST")) {
        		result.add(((Struct)x.get(i)).getString("code"));
        	}
        }

        return result;
    }

}