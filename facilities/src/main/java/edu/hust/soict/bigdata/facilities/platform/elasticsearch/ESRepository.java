package edu.hust.soict.bigdata.facilities.platform.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.facilities.common.config.Const;
import edu.hust.soict.bigdata.facilities.common.config.Properties;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import edu.hust.soict.bigdata.facilities.platform.kafka.KafkaBrokerWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class ESRepository<M extends DataModel> implements AutoCloseable{

    private RestHighLevelClient esClient;
    private Properties props;
    private KafkaBrokerWriter writer;

    private String topicOnFailure;

    private static final ObjectMapper om = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(ESRepository.class);

    public ESRepository(Properties props, String clientName){
        esClient = ElasticClientProvider.getOrCreate(clientName, new ElasticConfig(props));
        this.writer = new KafkaBrokerWriter(props);
        this.topicOnFailure = props.getProperty(Const.ELASTIC_KAFKA_TOPIC_ON_FAILURE);
        this.props = props;
    }

    public void add(M data, String index) throws JsonProcessingException {
        IndexRequest request = new IndexRequest()
                .index(index)
                .id(data.getId())
                .source(om.writeValueAsString(data), XContentType.JSON);

        esClient.indexAsync(request, RequestOptions.DEFAULT, new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                logger.info(indexResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e.getMessage());
                pushOnFailure(data);
            }
        });
    }

    public void bulkInsert(List<M> bulkData, String index){
        BulkRequest request = new BulkRequest();
        for(M data : bulkData){
            try {
                String source = om.writeValueAsString(data);
                request.add( new IndexRequest()
                        .index(index)
                        .id(data.getId())
                        .source(source, XContentType.JSON));
            } catch (JsonProcessingException e) {
                logger.warn("Cannot parse json to index to es:\n" + data);
            }
        }

        esClient.bulkAsync(request, RequestOptions.DEFAULT, new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                for(BulkItemResponse bulkItemResponse : bulkItemResponses){
                    if(bulkItemResponse.isFailed()){

                    }
                }
                List<String> responses = new LinkedList<>();
                bulkItemResponses.iterator().forEachRemaining(m -> responses.add(m.getId()));

                List<M> fails = bulkData.stream().filter(m -> responses.contains(m.getId())).collect(Collectors.toList());
                for(M data : bulkData){
                    pushOnFailure(data);
                }

                logger.info(bulkItemResponses.toString());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e.getMessage());
                for(M data : bulkData){
                    pushOnFailure(data);
                }
            }
        });
    }

    public M getOneById(String id, String index) throws IOException {
        GetRequest request = new GetRequest(index, id);
        GetResponse response = esClient.get(request, RequestOptions.DEFAULT);

        String source = response.getSourceAsString();
        return om.readValue(source, new TypeReference<M>() {});
    }

    public void deleteOneById(String id, String index){
        DeleteRequest request = new DeleteRequest(index, id);
        esClient.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                logger.info(deleteResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e.getMessage());
            }
        });
    }

    private void pushOnFailure(M data){
        try {
            writer.write(data.getId(), om.writeValueAsString(data), topicOnFailure);
        } catch (JsonProcessingException e1) {
            logger.error(e1.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        esClient.close();
    }
}
