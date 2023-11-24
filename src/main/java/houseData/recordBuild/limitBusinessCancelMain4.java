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

public class limitBusinessCancelMain4 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String Limit_ERROR_FILE="/limitBusinessCancelError.sql";
    private static final String Limit_FILE="/limitBusinessCancelRecord.sql";
    private static File limitCancelBusinessFileError;
    private static File limitCancelBusinessFile;
    private static BufferedWriter limitCancelBusinessWriterError;
    private static BufferedWriter limitCancelBusinessWriter;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    private static Statement buildStatement;
    private static ResultSet buildResultSet;

    private static Statement lockedHouseCancelStatement;
    private static ResultSet lockedHouseCancelResultSet;

    private static Statement projectStatement;
    private static ResultSet projectResultSet;


    public static void main(String agr[]) throws SQLException {

        limitCancelBusinessFileError = new File(Limit_ERROR_FILE);
        if (limitCancelBusinessFileError.exists()) {
            limitCancelBusinessFileError.delete();
        }

        limitCancelBusinessFile = new File(Limit_FILE);
        if (limitCancelBusinessFile.exists()) {
            limitCancelBusinessFile.delete();
        }

        try{
            limitCancelBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(limitCancelBusinessFile.getAbsoluteFile());
            limitCancelBusinessWriter = new BufferedWriter(fw);
            limitCancelBusinessWriter.write("USE record_building;");
            limitCancelBusinessWriter.newLine();
            limitCancelBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.limit.cancel.import','预警取消业务导入',false,true,0,'business');");

            limitCancelBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("limitCancelBusinessFile 文件创建失败");
            e.printStackTrace();
            return;
        }


        try{
            limitCancelBusinessFileError.createNewFile();
            FileWriter fw = new FileWriter(limitCancelBusinessFileError.getAbsoluteFile());
            limitCancelBusinessWriterError = new BufferedWriter(fw);
            limitCancelBusinessWriterError.write("limitBusiness--错误记录:");
            limitCancelBusinessWriterError.newLine();
            limitCancelBusinessWriterError.flush();
        }catch (IOException e){
            System.out.println("limitBusinessFileError 文件创建失败");
            e.printStackTrace();
            return;
        }

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);
        BuildIdMapper buildIdMapper = sqlSession.getMapper(BuildIdMapper.class);
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        LockedHouseCancelIdMapper lockedHouseCancelIdMapper = sqlSession.getMapper(LockedHouseCancelIdMapper.class);

        lockedHouseCancelStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        LockedHouseCancelId lockedHouseCancelId = null;
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        BuildId buildId =null;
        HouseId houseId = null;
        ProjectId projectId = null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null;

        try {
            lockedHouseCancelResultSet = lockedHouseCancelStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.LOCKED_HOUSE_CANCEL where HOUSE_CODE='110291' ORDER BY HOUSE_CODE");
            lockedHouseCancelResultSet.last();
            int sumCount = lockedHouseCancelResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            lockedHouseCancelResultSet.beforeFirst();
            while (lockedHouseCancelResultSet.next()){
                lockedHouseCancelId = lockedHouseCancelIdMapper.selectByOldId(lockedHouseCancelResultSet.getString("ID"));
                if (lockedHouseCancelId == null){
                    System.out.println("没有找到对应记录检查lockedHouseCancelId--:"+lockedHouseCancelResultSet.getString("ID"));
                    return;
                }
                houseId = houseIdMapper.selectByOldHouseId(lockedHouseCancelResultSet.getString("HOUSE_CODE"));
                if(houseId==null){
                    System.out.println("limitBusinessCancelMain4没有找到对应HOUSE_CODE记录检查:--:"+lockedHouseCancelResultSet.getString("HOUSE_CODE"));
                    return;
                }
                houseResultSet = houseStatement.executeQuery("SELECT * FROM HOUSE_INFO.HOUSE WHERE ID='"+lockedHouseCancelResultSet.getString("HOUSE_CODE")+"'");
                houseResultSet.next();
                buildId = buildIdMapper.selectByOldBuildId(houseResultSet.getString("BuildID"));
                if(buildId==null){
                    System.out.println("limitBusinessCancelMain4没有找到对应记录检查BuildID-:"+buildResultSet.getString("BuildID"));
                    return;
                }
                buildResultSet = buildStatement.executeQuery("SELECT * FROM HOUSE_INFO.BUILD WHERE ID='"+houseResultSet.getString("BuildID")+"'");
                buildResultSet.next();

                projectResultSet = projectStatement.executeQuery("SELECT P.*,A.LICENSE_NUMBER,D.NAME AS DNAME FROM HOUSE_INFO.PROJECT AS P " +
                        "LEFT JOIN HOUSE_INFO.DEVELOPER AS D ON P.DEVELOPERID=D.ID " +
                        "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS A ON D.ATTACH_ID=A.ID WHERE P.ID ='"+buildResultSet.getString("PROJECT_ID")+"'  ORDER BY P.NAME");

                projectResultSet.next();
                projectId = projectIdMapper.selectByOldProjectId(projectResultSet.getString("ID"));
                if(projectId == null){
                    System.out.println("limitBusinessCancelMain4没有找到对应记录检查PROJECT_ID--:"+buildResultSet.getString("PROJECT_ID"));
                    return;
                }
                //获取开发新ID=UNIFIED_ID 营业执照号，没有的用开发商新ID替代 没有开发商用未知开发商
                if(projectResultSet.getString("DEVELOPERID")!=null && !projectResultSet.getString("DEVELOPERID").isBlank()){
                    jointCorpDevelop = jointCorpDevelopMapper.selectByDevelopId(projectResultSet.getString("DEVELOPERID"));
                    UNIFIED_ID = projectResultSet.getString("LICENSE_NUMBER");
                    developName = projectResultSet.getString("DNAME");
                    if (UNIFIED_ID==null || UNIFIED_ID.isBlank()){
                        UNIFIED_ID = Long.toString(jointCorpDevelop.getCorpId());
                    }
                }else{
                    UNIFIED_ID ="0";
                    developName = "未知";
                }
                //work lockedHouseId 作为workId
                limitCancelBusinessWriter.newLine();
                limitCancelBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                limitCancelBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseCancelId.getId()),Q.pm("OLD")
                        ,Q.pm(lockedHouseCancelResultSet.getTimestamp("CANCEL_TIME")),Q.pm(lockedHouseCancelResultSet.getTimestamp("CANCEL_TIME"))
                        ,Q.pm("房屋预警导入"),Q.pm("COMPLETED")
                        ,Q.pm(lockedHouseCancelResultSet.getTimestamp("CANCEL_TIME")),Q.pm(lockedHouseCancelResultSet.getTimestamp("CANCEL_TIME"))
                        ,"0",Q.pm("func.limit.cancel.import")
                        ,"true",Q.pm("business")
                )+ ");");

                //work_operator
                limitCancelBusinessWriter.newLine();
                limitCancelBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                limitCancelBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseCancelId.getId()),Q.pm("TASK")
                        ,Q.pm(lockedHouseCancelResultSet.getString("EMP_CODE")),Q.pm(lockedHouseCancelResultSet.getString("EMP_NAME"))
                        ,Q.pm(lockedHouseCancelResultSet.getString("ID")),Q.pm(lockedHouseCancelResultSet.getTimestamp("CANCEL_TIME"))
                )+ ");");

                limitCancelBusinessWriter.newLine();
                limitCancelBusinessWriter.write("INSERT work_task (task_id, message, task_name, pass) VALUE ");
                limitCancelBusinessWriter.write("(" + Q.v(Q.pm(lockedHouseCancelResultSet.getString("ID")),Q.pm("同意")
                        ,Q.pm("建立"),Q.p(true)
                )+ ");");

                //limit_cancel_business
                limitCancelBusinessWriter.newLine();
                limitCancelBusinessWriter.write("insert limit_cancel_business (work_id, explanation, way, from_id) value ");
                limitCancelBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseCancelId.getId()),Q.pm(lockedHouseCancelResultSet.getString("DESCRIPTION"))
                        ,Q.pm("HOUSE"),Long.toString(buildId.getId())
                )+ ");");

                //house_freeze_cancel_business
                limitCancelBusinessWriter.newLine();
                limitCancelBusinessWriter.write("insert house_freeze_cancel_business (limit_id, work_id, target_id) value ");
                limitCancelBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseCancelId.getId()),Long.toString(lockedHouseCancelId.getId())
                        ,Long.toString(houseId.getId())
                )+ ");");

                //project_business
                limitCancelBusinessWriter.newLine();
                limitCancelBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                        "developer_name, work_type,business_id,before_info_id) VALUE ");
                limitCancelBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseCancelId.getId()),Long.toString(projectId.getId())
                        ,Q.pm(UNIFIED_ID),Long.toString(projectId.getId())
                        ,Q.pm(developName),Q.pm("REFER")
                        ,Long.toString(lockedHouseCancelId.getId()),Long.toString(projectId.getId())
                )+ ");");

                //build_business
                limitCancelBusinessWriter.newLine();
                limitCancelBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                limitCancelBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseCancelId.getId()),Long.toString(buildId.getId())
                        ,Long.toString(projectId.getId()),Q.pm(lockedHouseCancelResultSet.getTimestamp("CANCEL_TIME"))
                        ,Long.toString(buildId.getId()),Q.pm("REFER")
                        ,Long.toString(lockedHouseCancelId.getId()),Long.toString(buildId.getId())
                )+ ");");

                //house_business
                limitCancelBusinessWriter.newLine();
                limitCancelBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                limitCancelBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseCancelId.getId()),Long.toString(houseId.getId())
                        ,Long.toString(buildId.getId()),Q.pm(lockedHouseCancelResultSet.getTimestamp("CANCEL_TIME"))
                        ,Long.toString(houseId.getId()),Long.toString(lockedHouseCancelId.getId())
                        ,Q.pm("BUSINESS"),Long.toString(houseId.getId())
                )+ ");");



                limitCancelBusinessWriter.flush();
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }

        }catch (Exception e){
            System.out.println("id is errer-----id:"+lockedHouseCancelResultSet.getString("ID"));
            e.printStackTrace();
            return;
        }finally{
            if(houseResultSet!=null){
                houseResultSet.close();
            }
            if(houseStatement!=null){
                houseStatement.close();
            }
            if(buildResultSet!=null){
                buildResultSet.close();
            }
            if(buildStatement!=null){
                buildStatement.close();
            }
            if(lockedHouseCancelResultSet!=null){
                lockedHouseCancelResultSet.close();
            }
            if(lockedHouseCancelStatement!=null){
                lockedHouseCancelStatement.close();
            }
            if(projectResultSet!=null){
                projectResultSet.close();
            }
            if(projectStatement!=null){
                projectStatement.close();
            }
            sqlSession.close();
        }
    }

}
