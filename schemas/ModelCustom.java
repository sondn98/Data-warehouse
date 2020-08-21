import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.model.HiveModel;

import java.sql.Timestamp;
import java.math.BigDecimal;

@HiveModel(schema = "default", table = "test")
public class ModelCustom extends DataModel {

    public Timestamp c3;

    public Integer c1;

    public String c2;

    @Override
    public String toString() {
        try {
           return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "";
        }
    }
}