package com.utils;

import java.sql.*;

public class MyConnection{

//    private static String DB_SEAWEED_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement statement = null;
    private static Connection connection = null;


    public static Statement getStatement(String url,String user,String password){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url,user,password);
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            System.out.println("seaweedConnection successful");
            return statement;
        }catch (Exception e){
            System.out.println("seaweedConnection is errer");
            e.printStackTrace();
            return statement;
        }
    }
    public static void closeConnection() throws SQLException {
        if(statement !=null){
            statement.close();
        }
        if (connection!=null){
            connection.close();
        }

    }
}
