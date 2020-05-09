//package edu.hust.soict.bigdata.collector.worker.restful;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import edu.hust.bigdata.action.ActionCollect;
//import edu.hust.bigdata.common.config.Properties;
//import edu.hust.bigdata.model.impl.ECommerceReview;
//import edu.hust.soict.bigdata.facilities.common.config.Properties;
//
//import javax.ws.rs.Path;
//import javax.ws.rs.core.MediaType;
//import javax.ws.rs.core.Response;
//import java.io.IOException;
//
//@Path("/")
//public class API {
//
//    private static final ObjectMapper om = new ObjectMapper();
//    private static final Properties props = new Properties();
//
//    private static final ActionCollect<ECommerceReview> collector =
//            new ActionCollect<>(props.toSubProperties("E_COMMERCE_REVIEW"));
//
//    @GET
//    @Path("/ping")
//    @Produces("text/plain")
//    public Response ping(@QueryParam("query") String query) {
//        return Response.ok().status(200).type(MediaType.APPLICATION_JSON)
//                .entity("<h1>Hello World. I'm data collection system</h1>").type(MediaType.TEXT_HTML).build();
//    }
//
//    @POST
//    @Path("/add-ecommerce-review")
//    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
//    @Consumes(MediaType.APPLICATION_JSON + ";charset=utf-8")
//    public Response addECommerceReview(String json) {
//        try{
//            ECommerceReview review = om.readValue(json, ECommerceReview.class);
//            collector.handle(review);
//        } catch (IOException e) {
//            e.printStackTrace();
//            return Response.serverError().status(500).type(MediaType.APPLICATION_JSON).build();
//        }
//        return Response.ok().status(200).type(MediaType.APPLICATION_JSON).build();
//    }
//
//
//}
