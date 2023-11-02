package com.utils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 无用
 */
public class FindWorkBook {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/DB_PLAT_SYSTEM?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement bookStatement;
    private static ResultSet bookResultSet;

    public static String getWorkValue(String workId) throws SQLException {
        try {
            bookStatement = MyConnection.getStatement(DB_URL, "root", "dgsoft");
            bookResultSet = bookStatement.executeQuery("SELECT * FROM DB_PLAT_SYSTEM.WORD WHERE ID='" + workId + "'");
            if (bookResultSet.next() && bookResultSet.getString("_VALUE") != null) {
                return bookResultSet.getString("_VALUE");
            } else {
                return "未知";
            }
        }catch (Exception e){
            e.printStackTrace();
            return "未知";
        }finally {
           if (bookResultSet != null){
               bookResultSet.close();
           }
           if (bookStatement != null){
               bookStatement.close();
           }
           MyConnection.closeConnection();
        }
    }
}
