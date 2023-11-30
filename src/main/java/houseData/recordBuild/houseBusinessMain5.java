package houseData.recordBuild;


import com.bean.*;
import com.mapper.*;
import com.utils.FindWorkBook;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

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

    private static Statement projectCardStatement;
    private static ResultSet projectCardResultSet;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;


    private static Set<String> DEAL_DEFINE_ID= new HashSet<>();


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
            houseBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('process_sale_contract_import','商品房合同备案导入',false,true,0,'business');");
            houseBusinessWriter.newLine();
            houseBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('process_sale_contract_cancel_import','商品房合同备案撤消导入',false,true,0,'business');");
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
        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper = sqlSession.getMapper(OwnerRecordHouseIdMapper.class);
        OwnerRecordBuildIdMapper ownerRecordBuildIdMapper = sqlSession.getMapper(OwnerRecordBuildIdMapper.class);
        OwnerRecordProjectIdMapper ownerRecordProjectIdMapper = sqlSession.getMapper(OwnerRecordProjectIdMapper.class);

        houseBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectCardStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        LockedHouseId lockedHouseId= null;
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        BuildId buildId =null;
        HouseId houseId = null;
        ProjectId projectId = null;
        OwnerRecordHouseId ownerRecordHouseId=null;
        OwnerRecordProjectId ownerRecordProjectId = null;
        OwnerRecordBuildId ownerRecordBuildId=null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null,DEFINE_ID=null;
        String nowId=null,beforeId=null,EMP_NAME=null;


        try{
            houseResultSet = houseStatement.executeQuery("SELECT HH.ID AS HID,HH.BUILDID,HB.PROJECT_ID,HP.DEVELOPERID,HD.NAME,HC.LICENSE_NUMBER," +
                    "HP.NAME AS DNAME,HS.DISTRICT FROM " +
                    "HOUSE_INFO.HOUSE AS HH LEFT JOIN HOUSE_INFO.BUILD AS HB ON HH.BUILDID=HB.ID " +
                    "LEFT JOIN HOUSE_INFO.PROJECT AS HP ON HB.PROJECT_ID=HP.ID LEFT JOIN HOUSE_INFO.SECTION AS HS ON HP.SECTIONID=HS.ID " +
                    "LEFT JOIN HOUSE_INFO.DEVELOPER AS HD ON HP.DEVELOPERID=HD.ID " +
                    "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS HC ON HD.ATTACH_ID=HC.ID " +
                    "WHERE HH.ID IN ('B544N1-4-02','0020-25','0030-0');");
            houseResultSet.last();
            int sumCount = houseResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            houseResultSet.beforeFirst();
            while (houseResultSet.next()){

                projectId = projectIdMapper.selectByOldProjectId(houseResultSet.getString("PROJECT_ID"));
                if(projectId == null){
                    System.out.println("houseBusinessMain5没有找到对应记录检查PROJECT_ID--:"+houseResultSet.getString("PROJECT_ID"));
                    return;
                }

                //开发商编号
                if(houseResultSet.getString("DEVELOPERID")!=null && !houseResultSet.getString("DEVELOPERID").isBlank()){
                    jointCorpDevelop = jointCorpDevelopMapper.selectByDevelopId(houseResultSet.getString("DEVELOPERID"));
                    UNIFIED_ID = houseResultSet.getString("LICENSE_NUMBER");
                    developName = houseResultSet.getString("DNAME");
                    if (UNIFIED_ID==null || UNIFIED_ID.isBlank()){
                        UNIFIED_ID = Long.toString(jointCorpDevelop.getCorpId());
                    }
                }else{
                    UNIFIED_ID ="0";
                    developName = "未知";
                }
                buildId = buildIdMapper.selectByOldBuildId(houseResultSet.getString("BUILDID"));
                if(buildId==null){
                    System.out.println("houseBusinessMain5没有找到对应记录检查BUILDID:--:"+houseResultSet.getString("BUILDID"));
                    return;
                }
                houseId = houseIdMapper.selectByOldHouseId(houseResultSet.getString("HID"));
                if(houseId==null){
                    System.out.println("houseBusinessMain5没有找到对应HOUSE_idE记录检查:--:"+houseResultSet.getString("HID"));
                    return;
                }
                houseBusinessResultSet = houseBusinessStatement.executeQuery("SELECT O.ID as OID,O.DEFINE_ID,BH.*,H.ID AS houseBId,H.PROJECT_CODE,O.*" +
                        " FROM OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
                        "LEFT JOIN HOUSE H ON BH.AFTER_HOUSE=H.ID " +
                        "WHERE O.STATUS IN ('COMPLETE','COMPLETE_CANCEL','MODIFYING') AND DEFINE_ID NOT IN ('WP50') " +
                        "AND BH.BUSINESS_ID IS NOT NULL AND BH.HOUSE_CODE ='"+houseResultSet.getString("HID")+"'"+
                        "ORDER BY H.HOUSE_CODE,O.CREATE_TIME;");
                if(houseBusinessResultSet.next()){
                   houseBusinessResultSet.last();
                   int houseBusinessCont = houseBusinessResultSet.getRow(),j=0;
                    nowId = null;
                    beforeId  = null;

                    houseBusinessResultSet.beforeFirst();
                    while (houseBusinessResultSet.next()){
                        DEFINE_ID = houseBusinessResultSet.getString("DEFINE_ID");
                        if(DEFINE_ID.equals("WP42") || DEFINE_ID.equals("BL42") || DEFINE_ID.equals("WP43")|| DEFINE_ID.equals("WP40")){
                            nowId = houseBusinessResultSet.getString("houseBId");
                            if(beforeId == null){
                                beforeId = Long.toString(houseId.getId());
                            }
                            //HOUSE.PROJECT_CODE 查询预售许可证的业务的项目楼幢信息
                            projectCardResultSet = projectCardStatement.executeQuery("select P.ID,P.PROJECT_CODE,O.ID AS OID,B.ID AS BID FROM OWNER_BUSINESS AS O " +
                                    ",HOUSE_OWNER_RECORD.PROJECT AS P,HOUSE_OWNER_RECORD.PROJECT_SELL_INFO AS PSI,HOUSE_OWNER_RECORD.BUILD B " +
                                    "WHERE O.ID=P.BUSINESS AND P.ID=PSI.ID AND P.ID=B.PROJECT " +
                                    "AND  O.DEFINE_ID IN ('WP50') AND STATUS IN ('COMPLETE') and P.PROJECT_CODE ='"+houseBusinessResultSet.getString("PROJECT_CODE")+"'");
                            //查询到用HOUSE_OWNER_RECORD的表的ID主键 没有用HOUSE_INFO库的表id
                            if(projectCardResultSet.next()){
                                ownerRecordProjectId = ownerRecordProjectIdMapper.selectByOldId(projectCardResultSet.getString("ID"));
                                if(ownerRecordProjectId == null){
                                    System.out.println("houseBusinessMain5没有找到对应记录检查ownerRecordProjectId--:"+projectCardResultSet.getString("ID"));
                                    return;
                                }
                                ownerRecordBuildId = ownerRecordBuildIdMapper.selectByOldBuildId(projectCardResultSet.getString("BID"));
                                if(ownerRecordBuildId==null){
                                    System.out.println("houseBusinessMain5没有找到对应记录检查ownerRecordBuildId:"+projectCardResultSet.getString("BID"));
                                    return;
                                }

                            }else{//有备案无预售许可证D用空间库的
                                ownerRecordProjectId.setId(projectId.getId());
                                ownerRecordProjectId.setOid(projectId.getOid());
                                ownerRecordBuildId.setId(buildId.getId());
                                ownerRecordBuildId.setOid(buildId.getOid());
                            }
                            //house_owner_record.house.id = ownerRecordHouseId
                            ownerRecordHouseId = ownerRecordHouseIdMapper.selectByOldId(houseBusinessResultSet.getString("houseBId"));
                            if(ownerRecordHouseId==null){
                                System.out.println("houseBusinessMain5没有找到对应记录检查ownerRecordHouseId:"+houseBusinessResultSet.getString("houseBId"));
                                return;
                            }

                            if(DEFINE_ID.equals("WP42") || DEFINE_ID.equals("BL42")) {
                                //work ownerRecordHouseId.getId() 作为workId
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()), Q.pm("OLD")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , Q.pm("商品房合同备案导入"), Q.pm("COMPLETED")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , "0", Q.pm("process_sale_contract_import")
                                        , "true", Q.pm("business")
                                ) + ");");
                            }

                            if(DEFINE_ID.equals("WP43")){
                                //work ownerRecordHouseId.getId() 作为workId
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()), Q.pm("OLD")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , Q.pm("商品房合同备案撤消导入"), Q.pm("COMPLETED")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , "0", Q.pm("process_sale_contract_cancel_import")
                                        , "true", Q.pm("business")
                                ) + ");");
                            }

                            //操作人员记录
                            taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUSINESS_EMP WHERE BUSINESS_ID='" + houseBusinessResultSet.getString("OID") + "'");
                            if (taskOperBusinessResultSet.next()) {
                                taskOperBusinessResultSet.beforeFirst();
                                while (taskOperBusinessResultSet.next()) {

                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                                    houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()), Q.pm("TASK")
                                            , Q.pm(taskOperBusinessResultSet.getString("EMP_CODE")), Q.pm(taskOperBusinessResultSet.getString("EMP_NAME"))
                                            , Q.pm(taskOperBusinessResultSet.getString("ID")), Q.pm(taskOperBusinessResultSet.getTimestamp("OPER_TIME"))
                                    ) + ");");

                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT work_task (task_id, message, task_name, pass) VALUE ");
                                    houseBusinessWriter.write("(" + Q.v(Q.pm(taskOperBusinessResultSet.getString("ID")), Q.pm("同意")
                                            , Q.pm(FindWorkBook.getEmpType(taskOperBusinessResultSet.getString("TYPE"))), Q.p(true)
                                    ) + ");");
                                }
                            }






                            System.out.println("BIZID--"+houseBusinessResultSet.getString("OID")+"---houseCode--:"+houseBusinessResultSet.getString("HOUSE_CODE")+"----nowID--:"+nowId+"---beforeId--"+beforeId);
                            beforeId = nowId;

                        }
                        if(!DEFINE_ID.equals("WP42") && !DEFINE_ID.equals("BL42")  && !DEFINE_ID.equals("WP43")){//有初始登记的 添加初始 登记 没有备案,撤案的添加产权预警证明房子已经有人了

                            if(DEFINE_ID.equals("WP40")){

                            }else{
                                System.out.println("DEFINE_ID---" + DEFINE_ID);
                            }

                        }

                        j++;
                        System.out.println("BusinessCont--"+j+"/"+String.valueOf(houseBusinessCont));

                    }
                }
                houseBusinessWriter.flush();
                i++;
                System.out.println("sumCount---"+i+"/"+String.valueOf(sumCount));
            }

        }catch (Exception e){
            System.out.println("id is errer-----id:"+houseResultSet.getString("HID"));
            e.printStackTrace();
            return;
        }finally {
            if (projectResultSet!=null){
                projectResultSet.close();
            }
            if(projectStatement!=null){
                projectStatement.close();
            }
            MyConnection.closeConnection();

        }





    }
}
