package houseData.recordBuild.file;

import com.bean.BizFileId;
import com.bean.HouseId;
import com.bean.OwnerRecordHouseId;
import com.bean.OwnerRecordProjectId;
import com.mapper.BizFileIdMapper;
import com.mapper.HouseIdMapper;
import com.mapper.OwnerRecordHouseIdMapper;
import com.mapper.OwnerRecordProjectIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;
import com.utils.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class wp40FileMain9 {

    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/wp40File8.sql";
    private static File businessFileFile;
    private static BufferedWriter businessFileWriter;
    private static Statement projectBusinessStatement;
    private static ResultSet projectBusinessResultSet;

    private static Statement buildBusinessStatement;
    private static ResultSet buildBusinessResultSet;

    private static Statement projectStatement;
    private static ResultSet projectResultSet;

    private static Statement landEndTimeStatement;
    private static ResultSet landEndTimeResultSet;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;

    private static Statement houseBusinessStatement;
    private static ResultSet houseBusinessResultSet;

    public static void main(String agr[]) throws SQLException {

        businessFileFile = new File(PROJECT_FILE);
        if(businessFileFile.exists()){
            businessFileFile.delete();
        }

        try{
            businessFileFile.createNewFile();
            FileWriter fw = new FileWriter(businessFileFile.getAbsoluteFile());
            businessFileWriter = new BufferedWriter(fw);
            businessFileWriter.write("USE work;");
            businessFileWriter.flush();
        }catch (IOException e){
            System.out.println("businessFileWriter 文件创建失败");
            e.printStackTrace();
            return;
        }

        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        landEndTimeStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        OwnerRecordProjectIdMapper ownerRecordProjectIdMapper = sqlSession.getMapper(OwnerRecordProjectIdMapper.class);
        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper = sqlSession.getMapper(OwnerRecordHouseIdMapper.class);
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);
        BizFileIdMapper bizFileIdMapper = sqlSession.getMapper(BizFileIdMapper.class);
        OwnerRecordProjectId ownerRecordProjectId = null;
        OwnerRecordHouseId ownerRecordHouseId=null;
        BizFileId bizFileId = null;
        HouseId houseId = null;
        try {
            houseBusinessResultSet = houseBusinessStatement.executeQuery("SELECT O.ID as OID,O.DEFINE_ID,H.ID AS houseBId,H.HOUSE_CODE as HCODE " +
                    "FROM OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
                    "LEFT JOIN HOUSE H ON BH.AFTER_HOUSE=H.ID " +
                    "WHERE O.STATUS IN ('COMPLETE','COMPLETE_CANCEL','MODIFYING') AND DEFINE_ID IN ('WP42','BL42','WP43') " + //WP40
                    "AND BH.BUSINESS_ID IS NOT NULL "+ // AND O.ID='WP40-2018051001139'
                    "ORDER BY H.HOUSE_CODE,O.CREATE_TIME;");


        }catch (Exception e){
            e.printStackTrace();
        }finally {
            sqlSession.close();

            if (projectResultSet!=null){
                projectResultSet.close();
            }
            if(projectStatement!=null){
                projectStatement.close();
            }

            if (projectBusinessResultSet!=null){
                projectBusinessResultSet.close();
            }
            if(buildBusinessResultSet!=null){
                buildBusinessResultSet.close();
            }
            if(buildBusinessResultSet!=null){
                buildBusinessResultSet.close();
            }
            if(buildBusinessStatement!=null){
                buildBusinessStatement.close();
            }
            if(landEndTimeResultSet!=null){
                landEndTimeResultSet.close();
            }
            if(landEndTimeStatement!=null){
                landEndTimeStatement.close();
            }
            if(taskOperBusinessResultSet!=null){
                taskOperBusinessResultSet.close();
            }
            if(taskOperBusinessStatement!=null){
                taskOperBusinessStatement.close();
            }
            if(workbookResultSet!=null){
                workbookResultSet.close();
            }
            if(workbookStatement!=null){
                workbookStatement.close();
            }
            MyConnection.closeConnection();
        }
    }
}
