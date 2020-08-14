import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.facilities.model.DataModel;

import java.util.Date;

public class TestModel extends DataModel {

    public int c1;

    public String c2;

    public Date c3;

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return "";
        }
    }
}
