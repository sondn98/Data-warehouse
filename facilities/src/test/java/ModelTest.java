
import com.fasterxml.jackson.annotation.JsonFormat;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.model.HiveModel;

import java.sql.Timestamp;

@HiveModel
public class ModelTest extends DataModel {

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern="yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    public Timestamp ts;

    @Override
    public String toString() {
        return null;
    }
}
