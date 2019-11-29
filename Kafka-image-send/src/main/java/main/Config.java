package main;

/**
 * @author HPPranaa
 */

public class Config {

    // This class passes all the config info into the SendImage Class
    public String kafkaEndpoint;
    public String topic;
    public String incomingDirectory;
    ConsulConfig cc;

    public Config(ConsulConfig cc)throws Exception {
        this.cc = cc;
        kafkaEndpoint = this.cc.makeRequest(this.cc.endpoint,"/v1/kv/Image/kafkaEndpoint").getBody().asString();
        topic = this.cc.makeRequest(this.cc.endpoint,"/v1/kv/Image/topic").getBody().asString();
        incomingDirectory = this.cc.makeRequest(this.cc.endpoint,"/v1/kv/Image/incomingDirectory").getBody().asString();
    }
}
