package edu.hust.soict.bigdata.collector.worker.restful;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/")
public class Ping {

    @GET
    @Path("/ping")
    @Produces("text/plain")
    public Response ping() {
        return Response.ok().status(200).type(MediaType.APPLICATION_JSON)
                .entity("<h1>Hello World. I'm data ware house system</h1>").type(MediaType.TEXT_HTML).build();
    }

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


}
