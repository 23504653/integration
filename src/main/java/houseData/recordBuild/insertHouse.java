package houseData.recordBuild;

import com.utils.MyConnection;
import com.utils.Q;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class insertHouse {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/record_building?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/insertHouse.sql";

    private static File projectBusinessFile;
    private static BufferedWriter projectBusinessWriter;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    public static void main(String agr[]) throws SQLException {

        projectBusinessFile = new File(PROJECT_FILE);
        if(projectBusinessFile.exists()){
            projectBusinessFile.delete();
        }

        try{
            projectBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(projectBusinessFile.getAbsoluteFile());
            projectBusinessWriter = new BufferedWriter(fw);
            projectBusinessWriter.write("USE record_building;");
            projectBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("projectBusinessWriter 文件创建失败");
            e.printStackTrace();
            return;
        }

        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        try {
            houseResultSet = houseStatement.executeQuery("select * from record_building.house where build_id=12993" );
            houseResultSet.last();
            int sumCount = houseResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            houseResultSet.beforeFirst();
            while (houseResultSet.next()){
                //house
                projectBusinessWriter.newLine();
                projectBusinessWriter.write("insert house (house_id, build_id, updated_at, house_info_id, status, created_at, mapping_corp_id, mapping_corp_name, version) value ");
                projectBusinessWriter.write("(" + Q.v(Long.toString(houseResultSet.getInt("house_id")),Long.toString(houseResultSet.getInt("build_id"))
                        ,Q.p(houseResultSet.getTimestamp("updated_at")),Long.toString(houseResultSet.getInt("house_info_id"))
                        ,Q.pm(houseResultSet.getString("status")),Q.p(houseResultSet.getTimestamp("created_at"))
                        ,Q.p(houseResultSet.getString("mapping_corp_id")),Q.p(houseResultSet.getString("mapping_corp_name"))
                        ,"0"
                )+ ");");


                projectBusinessWriter.flush();
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));


            }

        }catch (Exception e){
            e.printStackTrace();
            return;
        }finally {

        }

    }


}
