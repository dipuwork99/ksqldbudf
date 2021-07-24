package com.example;

import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import org.apache.kafka.connect.data.Struct;
import java.util.ArrayList;
import java.util.List;
import com.example.*;

@UdtfDescription(name = "getcode_for_codetype",
                 author = "example user",
                 version = "1.5.0",
                 description = "From an alternate code array type retreieve the code for a given code type")
public class AlternateCodeExtractorUdtf {

  
    @Udtf(description = "Takes an array of alternate type  and returns the code value for the given code type.")
    public <E> List<String> getCodeForCodeType(@UdfParameter List<E> x , @UdfParameter String codeType) {
        List<String> result = new ArrayList<>();

        for(int i = 0; i < x.size(); i++) { 
        	if(((Struct)x.get(i)).getString("codeType").equals(codeType)) {
        		result.add(((Struct)x.get(i)).getString("code"));
        	}
        }

        return result;
    }

}