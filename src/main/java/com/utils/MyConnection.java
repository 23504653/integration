package com.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MyConnection {
    private static final String DB_SEAWEED_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement statement = null;
    private static Connection connection = null;
    static{
        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(DB_SEAWEED_URL,"root","dgsoft");
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            System.out.println("seaweedConnection successful");
        }catch (Exception e){
            System.out.println("seaweedConnection is errer");
            e.printStackTrace();
        }
    }
    public static Statement getStatement(){
        return statement;
    }

}
