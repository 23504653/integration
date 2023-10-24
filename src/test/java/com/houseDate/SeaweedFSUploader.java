package com.houseDate;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class SeaweedFSUploader {

    @Test
    public  void Uploader(){
        String seaweedFSUploadURL = "http://192.168.1.21:9333";
        String localFilePath = "E:\\docker-weedfs\\wxy.jpg";


        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPost = new HttpPost(seaweedFSUploadURL + "/submit");
            MultipartEntityBuilder builder = MultipartEntityBuilder.create();

            File imageFile = new File(localFilePath);
            builder.addBinaryBody("file", imageFile, ContentType.APPLICATION_OCTET_STREAM, imageFile.getName());

            httpPost.setEntity(builder.build());

            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity responseEntity = response.getEntity();

            if (responseEntity != null) {
                String responseBody = EntityUtils.toString(responseEntity);
                JsonParser jsonParser = new JsonParser();
                JsonObject jsonObject = jsonParser.parse(responseBody).getAsJsonObject();
                System.out.println("fid-"+jsonObject.get("fid").getAsString());
                System.out.println("Upload response: " + responseBody);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
