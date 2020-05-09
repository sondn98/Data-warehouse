//package edu.hust.soict.bigdata.batch.worker;
//
//import edu.hust.soict.bigdata.batch.BatchProcessingThread;
//import edu.hust.soict.bigdata.batch.common.BatchConst;
//import edu.hust.soict.bigdata.batch.model.ECommerceReview;
//import edu.hust.soict.bigdata.facilities.common.config.Properties;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//import java.util.Arrays;
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//public class MainCollector {
//
//    private static final Logger logger = LoggerFactory.getLogger(MainCollector.class);
//
//    private static List<BatchProcessingThread<?>> listProcessors(Properties props) throws IOException {
//        return Arrays.asList(
//                new BatchProcessingThread<ECommerceReview>(props.toSubProperties("E_COMMERCE_REVIEW"))
//        );
//    }
//
//    public static void main(String[] args) throws IOException {
//        Properties props = new Properties();
//        ExecutorService service =
//                Executors.newFixedThreadPool(props.getIntProperty(BatchConst.MAIN_COLLECTOR_THREAD_COUNT, 2));
//        List<BatchProcessingThread<?>> processors = listProcessors(props);
//
//        logger.info("Starting process threads");
//        for(BatchProcessingThread<?> processor : processors){
//            service.submit(processor);
//        }
//    }
//}
