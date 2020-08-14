import edu.hust.soict.bigdata.facilities.platform.hbase.HBaseConnectionProvider;
import edu.hust.soict.bigdata.facilities.platform.hbase.HbaseRepository;
import edu.hust.soict.bigdata.facilities.platform.hive.HiveRepository;

public class Test {

    public static void main(String[] args) throws ClassNotFoundException {
        HiveRepository<TestModel> hiveRepo = new HiveRepository<TestModel>() {};
    }
}
