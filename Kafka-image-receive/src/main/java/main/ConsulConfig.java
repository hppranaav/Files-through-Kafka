package main;

import java.util.Optional;
import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;
import static io.restassured.RestAssured.given;

public class ConsulConfig {

    public String endpoint;

    public ConsulConfig()throws Exception {
        endpoint = Optional.ofNullable(System.getenv("CONSUL_ENDPOINT")).orElseThrow(
                () -> new Exception("CONSUL_ENDPOINT has not been set as an env var"));
    }

    public Response makeRequest(String endpoint, String path) throws Exception {
        RestAssured.baseURI = endpoint;
        RestAssured.useRelaxedHTTPSValidation();
        RestAssured.given();

        RequestSpecification request;
        RequestSpecBuilder requestSpecBuilder =new RequestSpecBuilder();

        request = requestSpecBuilder.build();
        request = given().spec(request);
        request.contentType("application/json");
        request.queryParam("raw",true);

        Response response;
        response = request.get(path);

        return response;
    }

}
