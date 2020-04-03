package com.serverless;

import java.util.Collections;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.dax.client.dynamodbv2.AmazonDaxClient;
import com.amazon.dax.client.dynamodbv2.ClientConfig;
import com.amazon.dax.client.dynamodbv2.ClusterDaxClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class Handler
implements RequestHandler<Map<String, Object>, ApiGatewayResponse> {

    private static final Logger LOG = LogManager.getLogger(Handler.class);

    @Override
    public ApiGatewayResponse handleRequest(Map<String, Object> input,
        Context context) {

        LOG.info("received: {}", input);

//         AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
         
//         BasicAWSCredentials awsCreds = new BasicAWSCredentials();
//         client = AmazonDynamoDBClientBuilder.standard()
//             .withCredentials(new AWSStaticCredentialsProvider(awsCreds))
//             .withRegion(Regions.fromName("us-west-1")).build();

        
        ClientConfig daxConfig = new ClientConfig()
        .withEndpoints("caching-dynamodb.f9t23e.clustercfg.dax.usw1.cache.amazonaws.com:8111").withRegion("us-west-1");
        AmazonDaxClient client = new ClusterDaxClient(daxConfig);
        
        DynamoDB dynamoDB = new DynamoDB(client);

//        GetItemSpec spec = new GetItemSpec().withPrimaryKey("key",
//            "[LSCSClientV2Impl, firstDocumentQuery, (TeamSite/Templating/DCR/Type:=LComMessage/Messages) AND (TeamSite/Metadata/lang:=\"EN\"), en]");
// GetItemSpec spec = new GetItemSpec().withPrimaryKey("key",
// "[DealerServiceImpl, findDealerByDealerCode, 64503]");
// GetItemSpec spec = new GetItemSpec().withPrimaryKey("Name",
// "5ZBIEYBjPb3pNLBoOKtb4k2~");
        GetItemSpec spec = new GetItemSpec().withPrimaryKey("key",
        "[DisclaimersDAOImpl, getDisclaimerMap, en]");
        

        Table table = dynamoDB.getTable("ContentCache");
// Table table = dynamoDB.getTable("DealerCache");
// Table table = dynamoDB.getTable("CreativeType");

        Item outcome = null;
        int count = 500;
        long startTime = System.currentTimeMillis();
        for (int x = 0; x < count; x++) {
//            long startTimeX = System.currentTimeMillis();
            outcome = table.getItem(spec);
//            long endTimeX = System.currentTimeMillis();
//            System.out.println("x: " + x + ", " + (endTimeX - startTimeX));
        }
        long endTime = System.currentTimeMillis();
        System.out.println("Duration(" + count + " times): " + "Total: " + (endTime - startTime) + "ms, Avg: " + (endTime - startTime) / count + "ms");
        if (outcome != null) {
            System.out.println("getItem succeeded:\n" + outcome.toJSONPretty());
        }

        Response responseBody = new Response(
            "Go Serverless v1.x! Your function executed successfully!", input);
        return ApiGatewayResponse.builder().setStatusCode(200)
            .setObjectBody(outcome.toJSONPretty()).setHeaders(Collections
                .singletonMap("X-Powered-By", "AWS Lambda & serverless"))
            .build();
    }
}
