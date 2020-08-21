package edu.hust.soict.bigdata.collector.worker.restful.apis;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.collector.common.CollectorConst;
import edu.hust.soict.bigdata.collector.datacollection.CollectorJobManager;
import edu.hust.soict.bigdata.collector.datacollection.SchemaGenerator;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.apache.zookeeper.KeeperException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

public class CollectorAPIs {

    private static final ObjectMapper om = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(CollectorAPIs.class);

    @POST
    @Path("/define/schema")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    @Consumes(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response defineSchema(String schema) {
        try {
            SchemaGenerator.buildClass(new JSONObject(schema));
            return Response.ok(schema).status(CollectorConst.RESPONSE_CREATED).build();
        } catch (IOException e) {
            logger.error("Error while building schema class", e);
            return Response.status(CollectorConst.RESPONSE_INTERNAL_SERVER_ERROR).entity(e).build();
        }
    }

    @POST
    @Path("/collect")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    @Consumes(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public <T extends DataModel> Response collectData(String bodyStr) {
        JSONObject body = new JSONObject(bodyStr);
        try {
            T data = SchemaGenerator.newInstance(body.getString("name"), body.getJSONObject("data"));
            CollectorJobManager.getInstance().collect(data);
            return Response.ok(bodyStr).status(CollectorConst.RESPONSE_OK).build();
        } catch (ClassNotFoundException e) {
            logger.error("Error while collecting data", e);
            return Response.status(CollectorConst.RESPONSE_BAD_REQUEST).entity(e).build();
        } catch (IOException e){
            logger.error("Error while parsing data object", e);
            return Response.status(CollectorConst.RESPONSE_INTERNAL_SERVER_ERROR).entity(e).build();
        } catch (InterruptedException | KeeperException e) {
            logger.error("Error while get CollectorJobManager instance", e);
            return Response.status(CollectorConst.RESPONSE_INTERNAL_SERVER_ERROR).entity(e).build();
        }
    }
}
