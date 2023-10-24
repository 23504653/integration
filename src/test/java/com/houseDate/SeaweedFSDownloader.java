package com.houseDate;


import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class SeaweedFSDownloader {

    @Test
    public void SDownloadURL() {
        String seaweedFSDownloadURL = "http://192.168.1.21:9380";
        String fileId = "2,072b3d34db";
        String outputPath = "E:\\docker-weedfs\\weedfs\\111.jpg";

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(seaweedFSDownloadURL + "/"+ fileId);
            System.out.println(httpGet);

            HttpResponse response = httpClient.execute(httpGet);
            System.out.println(response.getStatusLine().getStatusCode());
            if (response.getStatusLine().getStatusCode() == 200) {
                InputStream content = response.getEntity().getContent();
                try (FileOutputStream fos = new FileOutputStream(outputPath)) {
                    int inByte;
                    while ((inByte = content.read()) != -1) {
                        fos.write(inByte);
                    }
                }
                System.out.println("Image downloaded to: " + outputPath);
            } else {
                System.out.println("Failed to download image. HTTP status code: " + response.getStatusLine().getStatusCode());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
