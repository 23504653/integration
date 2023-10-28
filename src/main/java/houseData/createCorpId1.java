package houseData;

import com.utils.MyConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class createCorpId1  {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_CROP_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement cropStatement;
    private static ResultSet cropResultSet;

    public static void Main(String agr[]) throws SQLException {

        cropStatement = MyConnection.getStatement(DB_CROP_URL,"root","dgsoft");

        try {
            cropResultSet = cropStatement.executeQuery("select * from HOUSE_INFO.HOUSE");

        }catch (Exception e){
            System.out.println("id is errer-----id:"+cropResultSet.getString("ID"));
            e.printStackTrace();
            return;
        }finally {
            if (cropResultSet!=null){
                cropResultSet.close();
            }
            if(cropResultSet!=null){
                cropResultSet.close();
            }
            MyConnection.closeConnection();

        }

    }

}
