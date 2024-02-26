package houseData.recordBuild;

import com.bean.OwnerRecordHouseId;
import com.bean.OwnerRecordProjectId;
import com.mapper.OwnerRecordHouseIdMapper;
import com.mapper.OwnerRecordProjectIdMapper;
import com.mapper.ProjectIdMapper;
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

public class deleteDefin {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/deleteDefine40.sql";
    private static File projectBusinessFile;
    private static BufferedWriter projectBusinessWriter;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;
    public static void main(String agr[]) throws SQLException {
//删除项目备案业务
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
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper = sqlSession.getMapper(OwnerRecordHouseIdMapper.class);
        OwnerRecordHouseId ownerRecordHouseId=null;

        try {
            workbookResultSet = workbookStatement.executeQuery("select rh.id,H.id as houseBId,bh.HOUSE_CODE from OWNER_BUSINESS as o left join BUSINESS_HOUSE as bh on o.ID=bh.BUSINESS_ID " +
                    "left join HOUSE AS H ON bh.AFTER_HOUSE=H.ID " +
                    "left join INTEGRATION.ownerRecordHouseId as rh on H.ID=rh.oid " +
                    "WHERE o.DEFINE_ID = 'WP40' and o.DEFINE_NAME='项目备案' and o.STATUS='COMPLETE';");


            workbookResultSet.last();
            int sumCount = workbookResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            workbookResultSet.beforeFirst();
            while (workbookResultSet.next()){
                ownerRecordHouseId = ownerRecordHouseIdMapper.selectByOldId(workbookResultSet.getString("houseBId"));
                if(ownerRecordHouseId==null){
                    System.out.println("deleteDefin没有找到对应记录检查ownerRecordHouseId:"+workbookResultSet.getString("houseBId"));
                    return;
                }
                projectBusinessWriter.newLine();
                projectBusinessWriter.write("update record_building.house_snapshot set register_info_id=null where register_info_id='"+ownerRecordHouseId.getId()+"';");

                projectBusinessWriter.newLine();
                projectBusinessWriter.write("delete from record_building.house_register_snapshot where register_info_id='"+ownerRecordHouseId.getId()+"';");

                projectBusinessWriter.flush();
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));

            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {

        }



    }

}
