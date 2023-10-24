package com.Json;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;


public class JsonReaderTest {

    @Test
    public void jsonChangString (){
        // JSON字符串示例
        String jsonString = "{\"name\":\"John\", \"age\":30, \"city\":\"New York\"}";
        System.out.println(jsonString);

        try {
            // 使用Gson库的JsonParser解析JSON字符串
            JsonParser jsonParser = new JsonParser();
            JsonObject jsonObject = jsonParser.parse(jsonString).getAsJsonObject();

            // 从JsonObject中获取数据
            String name = jsonObject.get("name").getAsString();
            int age = jsonObject.get("age").getAsInt();
            String city = jsonObject.get("city").getAsString();

            System.out.println("Name: " + name);
            System.out.println("Age: " + age);
            System.out.println("City: " + city);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
