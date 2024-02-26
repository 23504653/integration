package houseData.recordBuild;

import com.bean.HouseId;
import com.bean.OwnerRecordHouseId;
import com.mapper.HouseIdMapper;
import com.mapper.OwnerRecordHouseIdMapper;
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

public class csdjUpdateInfo {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/csdjUpdateInfo.sql";
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
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        HouseId houseId = null;
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);
        try {
            houseResultSet = houseStatement.executeQuery("select rh.id,H.id as houseBId,bh.HOUSE_CODE from OWNER_BUSINESS as o left join BUSINESS_HOUSE as bh on o.ID=bh.BUSINESS_ID " +
                    "left join HOUSE AS H ON bh.AFTER_HOUSE=H.ID " +
                    "left join INTEGRATION.ownerRecordHouseId as rh on H.ID=rh.oid " +
                    "WHERE o.DEFINE_ID = 'WP40' and o.STATUS='COMPLETE';");

            houseResultSet.last();
            int sumCount = houseResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            houseResultSet.beforeFirst();
            while (houseResultSet.next()){
                houseId = houseIdMapper.selectByOldHouseId(houseResultSet.getString("HOUSE_CODE"));
                if(houseId==null){
                    System.out.println("houseBusinessMain5没有找到对应HOUSE_idE记录检查:--:"+houseResultSet.getString("HOUSE_CODE"));
                    return;
                }
                workbookResultSet =  workbookStatement.executeQuery("select * from record_building.new_house_contract_business where house_id='"+houseId.getId()+"'");
                workbookResultSet.last();

                if (workbookResultSet.getRow()<=0){

                    taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("select * from record_building.house where house_id = '"+houseId.getId()+"' and house_info_id<> '"+houseId.getId()+"'" );
                    taskOperBusinessResultSet.last();
                    if (taskOperBusinessResultSet.getRow()>0){
                        System.out.println(workbookResultSet.getRow()+"----"+houseId.getId());
                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("update record_building.house set house_info_id='"+houseId.getId()+"' where house_id='"+houseId.getId()+"';");
                        projectBusinessWriter.flush();
                    }


                }



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
