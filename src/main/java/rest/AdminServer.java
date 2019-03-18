package rest;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

public class AdminServer {

    private final static int port = 9998;
    private final static String host="http://localhost/";

    public static void main(String[] args) {
        URI baseUri = UriBuilder.fromUri(host).port(port).build();
        ResourceConfig config = new ResourceConfig(AdminKeeper.class);
        JdkHttpServerFactory.createHttpServer(baseUri, config);
    }
}
