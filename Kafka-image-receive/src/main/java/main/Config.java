package main;

/**
 * @author HPPranaa
 */

public class Config {

    // This class passes all the config info into the ReceiveImage Class
    public String kafkaEndpoint;
    public String topic;
    public String outgoingDirectory;
    public String groupID;
    ConsulConfig cc;

    public Config(ConsulConfig cc)throws Exception {
        this.cc = cc;
        kafkaEndpoint = this.cc.makeRequest(this.cc.endpoint,"/v1/kv/Image/kafkaEndpoint").getBody().asString();
        topic = this.cc.makeRequest(this.cc.endpoint,"/v1/kv/Image/topic").getBody().asString();
        outgoingDirectory = this.cc.makeRequest(this.cc.endpoint,"/v1/kv/Image/outgoingDirectory").getBody().asString();
        groupID =this.cc.makeRequest(this.cc.endpoint,"/v1/kv/Image/groupid").getBody().asString();
    }
}
