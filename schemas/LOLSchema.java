import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.model.HiveModel;

import java.sql.Timestamp;
import java.math.BigDecimal;

@HiveModel(schema = "default", table = "lol")
public class LOLSchema extends DataModel {

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern="yyyy-MM-dd'T'HH:mm:ss.SSSZ")
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