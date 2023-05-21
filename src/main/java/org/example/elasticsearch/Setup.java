/*package org.example.elasticsearch;


import org.apache.http.HttpHost;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.client.RestClient;

public class Setup {
    public static void main(){
        RestClient restClient = RestClient.builder(
                new HttpHost("localhost", 9200)).build();

// Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

// And create the API client
        ElasticsearchClient client = new ElasticsearchClient(transport);
    }
}
*/