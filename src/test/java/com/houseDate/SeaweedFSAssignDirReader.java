package com.houseDate;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

public class SeaweedFSAssignDirReader {
    @Test
    public void AssignDirReader() {
        String seaweedFSAssignURL = "http://localhost:9333/dir/assign"; // SeaweedFS的/dir/assign端点

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(seaweedFSAssignURL);

            HttpResponse response = httpClient.execute(httpGet);

            if (response.getStatusLine().getStatusCode() == 200) {
                String responseBody = EntityUtils.toString(response.getEntity());
                System.out.println("JSON Response from SeaweedFS Assign Dir: " + responseBody);
            } else {
                System.out.println("Failed to fetch JSON data. HTTP status code: " + response.getStatusLine().getStatusCode());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
