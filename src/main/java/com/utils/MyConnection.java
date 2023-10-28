package com.utils;

import java.sql.*;

public class MyConnection{

    private static Statement statement = null;
    private static Connection connection = null;


    public static Statement getStatement(String url,String user,String password){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection(url,user,password);
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            System.out.println("connection successful");
            return statement;
        }catch (Exception e){
            System.out.println("connection is errer");
            e.printStackTrace();
            return statement;
        }
    }
    public static void closeConnection() throws SQLException {
        if (connection!=null){
            connection.close();
        }

    }
}
