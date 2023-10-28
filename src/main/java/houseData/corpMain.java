package houseData;

import com.utils.MyConnection;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class corpMain {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_CROP_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static final String CORP_ERROR_FILE="/corpError.sql";
    private static final String CORP_FILE="/corpRecord.sql";
    private static BufferedWriter cropWriterError;
    private static BufferedWriter cropWriter;
    private static File cropFileError;
    private static File cropFile;
    private static Statement cropStatement;
    private static ResultSet cropResultSet;

    public static void main(String agr[]) throws SQLException {
        cropFileError = new File(CORP_ERROR_FILE);
        if(cropFileError.exists()){
            cropFileError.delete();
        }
        cropFile = new File(CORP_FILE);
        if(cropFile.exists()){
            cropFile.delete();
        }

        try{
            cropFile.createNewFile();
            FileWriter fw = new FileWriter(cropFile.getAbsoluteFile());
            cropWriter = new BufferedWriter(fw);
            cropWriter.write("USE record_corp;");
            cropWriter.newLine();
            cropWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.corp.record.import','从业机构导入',false,true,0,'data');");
            cropWriter.flush();
        }catch (IOException e){
            System.out.println("cropWriter 文件创建失败");
            e.printStackTrace();
            return;
        }


        try{
            cropFileError.createNewFile();
            FileWriter fw = new FileWriter(cropFileError.getAbsoluteFile());
            cropWriterError = new BufferedWriter(fw);
            cropWriterError.write("corp--错误记录:");
            cropWriterError.newLine();
            cropWriterError.flush();
        }catch (IOException e){
            System.out.println("cropWriterError 文件创建失败");
            e.printStackTrace();
            return;
        }
        cropStatement = MyConnection.getStatement(DB_CROP_URL,"root","dgsoft");

        try {
            cropResultSet = cropStatement.executeQuery("");


        }catch (Exception e){
            System.out.println("id is errer-----id:"+cropResultSet.getString("id"));
            e.printStackTrace();
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
