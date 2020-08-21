package edu.hust.soict.bigdata.collector.worker.restful.apis;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.hust.soict.bigdata.collector.common.CollectorConst;
import edu.hust.soict.bigdata.collector.datacollection.CollectorJobAttributes;
import edu.hust.soict.bigdata.collector.datacollection.CollectorJobManager;
import edu.hust.soict.bigdata.facilities.common.config.Config;
import edu.hust.soict.bigdata.facilities.common.util.Strings;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Path("/collection")
public class JobAPIs {

    private static final ObjectMapper om = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(JobAPIs.class);

    @POST
    @Path("/job-collect/define")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    @Consumes(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response defineJobCollect(String body) {
        try {
            CollectorJobAttributes attributes = om.readValue(body, CollectorJobAttributes.class);
            synchronized (this){
                Config.setRuntimeObj(CollectorConst.COLLECTOR_JOB_ATTR_OBJECT_KEY, attributes);
                CollectorJobManager manager = CollectorJobManager.getInstance();
                manager.getOrCreate(attributes.SCHEMA_NAME);
                manager.start(attributes.SCHEMA_NAME, null);
            }

            return Response.ok().status(CollectorConst.RESPONSE_CREATED).build();
        } catch (IOException e) {
            logger.error("Error while parsing attributes", e);
            return Response.status(CollectorConst.RESPONSE_BAD_REQUEST).entity(e).build();
        } catch (KeeperException | InterruptedException e){
            logger.error("Error while get CollectorJobManager instance", e);
            return Response.status(CollectorConst.RESPONSE_INTERNAL_SERVER_ERROR).entity(e).build();
        }
    }

    @PUT
    @Path("/job-collect/stop/{name}")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    @Consumes(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response stopJobCollect(@PathParam("name") String name) {
        try {
            synchronized (this){
                CollectorJobManager manager = CollectorJobManager.getInstance();
                manager.stop(name, false);
            }

            return Response.ok().status(CollectorConst.RESPONSE_CREATED).build();
        } catch (IOException e) {
            logger.error("Error while parsing attributes", e);
            return Response.status(CollectorConst.RESPONSE_BAD_REQUEST).entity(e).build();
        } catch (Exception e) {
            logger.error("Error while stopping job", e);
            return Response.status(CollectorConst.RESPONSE_INTERNAL_SERVER_ERROR).entity(e).build();
        }
    }

    @PUT
    @Path("/job-collect/restart/{name}")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    @Consumes(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response restartJobCollect(String body, @PathParam("name") String name) {
        try {
            CollectorJobAttributes attributes = Strings.isNullOrEmpty(body) ?
                    null : om.readValue(body, CollectorJobAttributes.class);
            synchronized (this){
                CollectorJobManager manager = CollectorJobManager.getInstance();
                manager.restart(name, attributes);
            }

            return Response.ok().status(CollectorConst.RESPONSE_CREATED).build();
        } catch (IOException e) {
            logger.error("Error while parsing attributes", e);
            return Response.status(CollectorConst.RESPONSE_BAD_REQUEST).entity(e).build();
        } catch (Exception e) {
            logger.error("Error while stopping job", e);
            return Response.status(CollectorConst.RESPONSE_INTERNAL_SERVER_ERROR).entity(e).build();
        }
    }

    @PUT
    @Path("/job-collect/remove/{name}")
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    @Consumes(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response removeJobCollect(String body, @PathParam("name") String name) {
        try {
            CollectorJobAttributes attributes = Strings.isNullOrEmpty(body) ?
                    null : om.readValue(body, CollectorJobAttributes.class);
            synchronized (this){
                CollectorJobManager manager = CollectorJobManager.getInstance();
                manager.restart(name, attributes);
            }

            return Response.ok().status(CollectorConst.RESPONSE_CREATED).build();
        } catch (IOException e) {
            logger.error("Error while parsing attributes", e);
            return Response.status(CollectorConst.RESPONSE_BAD_REQUEST).entity(e).build();
        } catch (Exception e) {
            logger.error("Error while stopping job", e);
            return Response.status(CollectorConst.RESPONSE_INTERNAL_SERVER_ERROR).entity(e).build();
        }
    }



}
