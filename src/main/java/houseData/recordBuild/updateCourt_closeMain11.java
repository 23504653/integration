package houseData.recordBuild;

import com.bean.*;
import com.mapper.*;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import com.utils.Q;
import org.apache.ibatis.session.SqlSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class updateCourt_closeMain11 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String Limit_FILE="/updateCourt11.sql";
    private static File limitOwnerBusinessFile;
    private static BufferedWriter limitOwnerBusinessWriter;


    private static Statement houseRecordStatement;
    private static ResultSet houseRecordResultSet;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    private static Statement buildStatement;
    private static ResultSet buildResultSet;

    private static Statement projectStatement;
    private static ResultSet projectResultSet;

    public static void main(String agr[]) throws SQLException {
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

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);
        BuildIdMapper buildIdMapper = sqlSession.getMapper(BuildIdMapper.class);
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        HouseRecordIdMapper houseRecordIdMapper = sqlSession.getMapper(HouseRecordIdMapper.class);


        buildStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        HouseRecordId houseRecordId = null;
        BuildId buildId =null;
        HouseId houseId = null;
        ProjectId projectId = null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null;
        String developer_info_id = null;
        houseRecordStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        try {
            houseRecordResultSet = houseRecordStatement.executeQuery("select HR.* FROM HOUSE_OWNER_RECORD.HOUSE_RECORD AS HR LEFT JOIN HOUSE_INFO.HOUSE AS H ON HR.HOUSE_CODE=H.ID " +
                    "WHERE (HR.HOUSE_STATUS='OWNERED' OR HR.HOUSE_STATUS='COURT_CLOSE') AND H.ID IS NOT NULL; "); //AND HR.HOUSE_CODE ='0021-0'
            houseRecordResultSet.last();
            int sumCount = houseRecordResultSet.getRow(), i = 0;
            System.out.println("记录总数-" + sumCount);
            houseRecordResultSet.beforeFirst();
            while (houseRecordResultSet.next()) {

                houseRecordId = houseRecordIdMapper.selectByOid(houseRecordResultSet.getString("HOUSE_CODE"));
                if(houseRecordId==null){
                    System.out.println("limitOwnerBusinessMain6没有找到对应houseRecordId记录检查:--:"+houseRecordResultSet.getString("HOUSE_CODE"));
                    return;
                }
                if(houseRecordResultSet.getString("HOUSE_STATUS").equals("COURT_CLOSE")){
                    limitOwnerBusinessWriter.newLine();
                    limitOwnerBusinessWriter.write("update work set work_name = '查封建立预警' WHERE work_id='" + houseRecordId.getId() + "';");

                    limitOwnerBusinessWriter.newLine();
                    limitOwnerBusinessWriter.write("update work_task set message = '房屋查封建立预警' WHERE task_id='" + houseRecordId.getId() + "';");

                    limitOwnerBusinessWriter.newLine();
                    limitOwnerBusinessWriter.write("update limit_business set explanation = '"+"由管理员将原系统查封的房屋建立预警！ 原房屋编号："+houseRecordResultSet.getString("HOUSE_CODE")+"' WHERE work_id='" + houseRecordId.getId() + "';");

                    limitOwnerBusinessWriter.newLine();
                    limitOwnerBusinessWriter.write("update sale_limit set explanation = '"+"{\"explanation\":\""+"由管理员将原系统查封的房屋建立预警！原房屋编号："+houseRecordResultSet.getString("HOUSE_CODE")+"\"}"+"' WHERE limit_id='" + houseRecordId.getId() + "';");



                }else {
                    limitOwnerBusinessWriter.newLine();
                    limitOwnerBusinessWriter.write("update work set work_name = '已办产权建立预警' WHERE work_id='" + houseRecordId.getId() + "';");

                    limitOwnerBusinessWriter.newLine();
                    limitOwnerBusinessWriter.write("update work_task set message = '已办产权建立预警' WHERE task_id='" + houseRecordId.getId() + "';");

                    limitOwnerBusinessWriter.newLine();
                    limitOwnerBusinessWriter.write("update limit_business set explanation = '"+"由管理员将原系统已办产权的房屋建立预警！ 原房屋编号："+houseRecordResultSet.getString("HOUSE_CODE")+"' WHERE work_id='" + houseRecordId.getId() + "';");

                    limitOwnerBusinessWriter.newLine();
                    limitOwnerBusinessWriter.write("update sale_limit set explanation = '"+"{\"explanation\":\""+"由管理员将原系统已办产权的房屋建立预警！原房屋编号："+houseRecordResultSet.getString("HOUSE_CODE")+"\"}"+"' WHERE limit_id='" + houseRecordId.getId() + "';");

                }

                limitOwnerBusinessWriter.flush();
                i++;
                System.out.println(i + "/" + String.valueOf(sumCount));

            }

        }catch (Exception e){
            e.printStackTrace();

        }finally {

            if(houseRecordResultSet!=null){
                houseRecordResultSet.close();
            }
            if(houseRecordStatement!=null){
                houseRecordStatement.close();
            }
        }




    }
}
