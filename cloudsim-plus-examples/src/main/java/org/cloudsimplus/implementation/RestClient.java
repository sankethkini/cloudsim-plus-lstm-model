package org.cloudsimplus.implementation;

import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class RestClient {
    private static final String RESOURCE_PATH="http://localhost:8000/api/abc";
    private RestTemplate restTemplate;

    public RestClient(RestTemplate restTemplate){
        this.restTemplate=restTemplate;
    }

    public ResponseEntity<ResponseFromServer> getCPUUsage(RequestToServer request){
        return this.restTemplate.postForEntity(RESOURCE_PATH,request,ResponseFromServer.class);
    }

}
