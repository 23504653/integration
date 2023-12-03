package houseData.recordBuild;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;

public class limitOwnerBusinessMain6 {

    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String Limit_ERROR_FILE="/limitOwnerBusinessError.sql";
    private static final String Limit_FILE="/limitOwnerBusinessRecord.sql";
    private static File limitOwnerBusinessFileError;
    private static File limitOwnerBusinessFile;
    private static BufferedWriter limitOwnerBusinessWriterError;
    private static BufferedWriter limitOwnerBusinessWriter;

    public static void main(String agr[]) throws SQLException {

        limitOwnerBusinessFileError = new File(Limit_ERROR_FILE);
        if (limitOwnerBusinessFileError.exists()) {
            limitOwnerBusinessFileError.delete();
        }

        limitOwnerBusinessFile = new File(Limit_FILE);
        if (limitOwnerBusinessFile.exists()) {
            limitOwnerBusinessFile.delete();
        }

        try{
            limitOwnerBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(limitOwnerBusinessFile.getAbsoluteFile());
            limitOwnerBusinessWriter = new BufferedWriter(fw);
            limitOwnerBusinessWriter.write("USE record_building;");
            limitOwnerBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("limitOwnerBusinessFile 文件创建失败");
            e.printStackTrace();
            return;
        }


        try{
            limitOwnerBusinessFileError.createNewFile();
            FileWriter fw = new FileWriter(limitOwnerBusinessFileError.getAbsoluteFile());
            limitOwnerBusinessWriterError = new BufferedWriter(fw);
            limitOwnerBusinessWriterError.write("limitBusiness--错误记录:");
            limitOwnerBusinessWriterError.newLine();
            limitOwnerBusinessWriterError.flush();
        }catch (IOException e){
            System.out.println("limitBusinessFileError 文件创建失败");
            e.printStackTrace();
            return;
        }





    }

}
