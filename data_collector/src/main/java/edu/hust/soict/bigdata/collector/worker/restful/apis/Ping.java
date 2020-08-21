package edu.hust.soict.bigdata.collector.worker.restful.apis;

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
        return Response.ok().status(200)
                .type(MediaType.APPLICATION_JSON)
                .entity("<h1 style=\"text-align:center\">Hello World. I'm data warehouse system</h1>")
                .type(MediaType.TEXT_HTML)
                .build();
    }


}
