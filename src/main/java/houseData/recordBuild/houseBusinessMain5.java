package houseData.recordBuild;


import com.bean.*;
import com.mapper.*;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class houseBusinessMain5 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String house_ERROR_FILE="/houseBusinessError.sql";
    private static final String house_FILE="/hoseBusinessRecord.sql";
    private static File houseBusinessFileError;
    private static File houseBusinessFile;
    private static BufferedWriter houseBusinessWriterError;
    private static BufferedWriter houseBusinessWriter;

    private static Statement houseBusinessStatement;
    private static ResultSet houseBusinessResultSet;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    private static Statement buildStatement;
    private static ResultSet buildResultSet;

    private static Statement projectStatement;
    private static ResultSet projectResultSet;


    public static void main(String agr[]) throws SQLException {
        houseBusinessFileError = new File(house_ERROR_FILE);
        if (houseBusinessFileError.exists()) {
            houseBusinessFileError.delete();
        }

        houseBusinessFile = new File(house_FILE);
        if (houseBusinessFile.exists()) {
            houseBusinessFile.delete();
        }

        try{
            houseBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(houseBusinessFile.getAbsoluteFile());
            houseBusinessWriter = new BufferedWriter(fw);
            houseBusinessWriter.write("USE record_building;");
            houseBusinessWriter.newLine();
            houseBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.limit.import','预警业务导入',false,true,0,'business');");

            houseBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("houseBusinessFile 文件创建失败");
            e.printStackTrace();
            return;
        }
        try{
            houseBusinessFileError.createNewFile();
            FileWriter fw = new FileWriter(houseBusinessFileError.getAbsoluteFile());
            houseBusinessWriterError = new BufferedWriter(fw);
            houseBusinessWriterError.write("houseBusiness--错误记录:");
            houseBusinessWriterError.newLine();
            houseBusinessWriterError.flush();
        }catch (IOException e){
            System.out.println("limitBusinessFileError 文件创建失败");
            e.printStackTrace();
            return;
        }

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        LockedHouseIdMapper lockedHouseIdMapper = sqlSession.getMapper(LockedHouseIdMapper.class);
        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);
        BuildIdMapper buildIdMapper = sqlSession.getMapper(BuildIdMapper.class);
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);

        houseBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        LockedHouseId lockedHouseId= null;
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        BuildId buildId =null;
        HouseId houseId = null;
        ProjectId projectId = null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null,DEFINE_ID=null;
        String nowId=null,beforeId=null;
        // 保存前一条记录的 house_code
        String previousHouseCode = null;

        try{
            houseBusinessResultSet = houseBusinessStatement.executeQuery("SELECT O.ID,O.DEFINE_ID,BH.* FROM OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
                    "LEFT JOIN HOUSE H ON BH.AFTER_HOUSE=H.ID " +
                    "WHERE O.STATUS IN ('COMPLETE','COMPLETE_CANCEL','MODIFYING') AND DEFINE_ID NOT IN ('WP50') " +
                    "AND BH.BUSINESS_ID IS NOT NULL AND BH.HOUSE_CODE IN ('B544N1-4-02','0020-25','0030-0')" +
                    "ORDER BY H.HOUSE_CODE,O.CREATE_TIME;");
            houseBusinessResultSet.last();
            int sumCount = houseBusinessResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            houseBusinessResultSet.beforeFirst();
            while(houseBusinessResultSet.next()){
                DEFINE_ID = houseBusinessResultSet.getString("DEFINE_ID");
                // 获取当前记录的 house_code
                String currentHouseCode = houseBusinessResultSet.getString("HOUSE_CODE");

                if (previousHouseCode == null || !previousHouseCode.equals(currentHouseCode)) {

                    System.out.println("当前currentHouseCode： " + currentHouseCode+"--- 钱一手previousHouseCode： " + previousHouseCode);
                    System.out.println();
                }

                    previousHouseCode = currentHouseCode;

                if(DEFINE_ID.equals("WP42") || DEFINE_ID.equals("BL42")){

                }




                houseBusinessWriter.flush();
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));

            }

        }catch (Exception e){

        }finally {

        }





    }
}
