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

public class limitBusinessMain3 {

    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String Limit_ERROR_FILE="/limitBusinessError3.sql";
    private static final String Limit_FILE="/limitBusinessRecord3.sql";
    private static File limitBusinessFileError;
    private static File limitBusinessFile;
    private static BufferedWriter limitBusinessWriterError;
    private static BufferedWriter limitBusinessWriter;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    private static Statement buildStatement;
    private static ResultSet buildResultSet;

    private static Statement lockedHouseStatement;
    private static ResultSet lockedHouseResultSet;

    private static Statement projectStatement;
    private static ResultSet projectResultSet;

    public static void main(String agr[]) throws SQLException {

        limitBusinessFileError = new File(Limit_ERROR_FILE);
        if (limitBusinessFileError.exists()) {
            limitBusinessFileError.delete();
        }

        limitBusinessFile = new File(Limit_FILE);
        if (limitBusinessFile.exists()) {
            limitBusinessFile.delete();
        }

        try{
            limitBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(limitBusinessFile.getAbsoluteFile());
            limitBusinessWriter = new BufferedWriter(fw);
            limitBusinessWriter.write("USE record_building;");
//            limitBusinessWriter.newLine();
//            limitBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.limit.create','预警业务导入',false,true,0,'business');");

            limitBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("limitBusinessFile 文件创建失败");
            e.printStackTrace();
            return;
        }


        try{
            limitBusinessFileError.createNewFile();
            FileWriter fw = new FileWriter(limitBusinessFileError.getAbsoluteFile());
            limitBusinessWriterError = new BufferedWriter(fw);
            limitBusinessWriterError.write("limitBusiness--错误记录:");
            limitBusinessWriterError.newLine();
            limitBusinessWriterError.flush();
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

        lockedHouseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        LockedHouseId lockedHouseId= null;
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        BuildId buildId =null;
        HouseId houseId = null;
        ProjectId projectId = null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null;
        try {

            lockedHouseResultSet = lockedHouseStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.LOCKED_HOUSE ORDER BY HOUSE_CODE");
            lockedHouseResultSet.last();
            int sumCount = lockedHouseResultSet.getRow(),i=0;

            System.out.println("记录总数-"+sumCount);
            lockedHouseResultSet.beforeFirst();
            while (lockedHouseResultSet.next()){
                lockedHouseId = lockedHouseIdMapper.selectByOldId(lockedHouseResultSet.getString("ID"));
                if (lockedHouseId == null){
                    System.out.println("没有找到对应记录检查lockedHouseId--:"+lockedHouseResultSet.getString("ID"));
                    return;
                }

                houseId = houseIdMapper.selectByOldHouseId(lockedHouseResultSet.getString("HOUSE_CODE"));
                if(houseId==null){
                    System.out.println("limitBusinessMain没有找到对应HOUSE_CODE记录检查:--:"+lockedHouseResultSet.getString("HOUSE_CODE"));
                    return;
                }

                houseResultSet = houseStatement.executeQuery("SELECT * FROM HOUSE_INFO.HOUSE WHERE ID='"+lockedHouseResultSet.getString("HOUSE_CODE")+"'");
                houseResultSet.next();
                buildId = buildIdMapper.selectByOldBuildId(houseResultSet.getString("BuildID"));
                if(buildId==null){
                    System.out.println("limitBusinessMain没有找到对应记录检查BuildID-:"+buildResultSet.getString("BuildID"));
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
                    System.out.println("limitBusinessMain没有找到对应记录检查PROJECT_ID--:"+buildResultSet.getString("PROJECT_ID"));
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
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseId.getId()),Q.pm("OLD")
                        ,Q.pm(lockedHouseResultSet.getTimestamp("LOCKED_TIME")),Q.pm(lockedHouseResultSet.getTimestamp("LOCKED_TIME"))
                        ,Q.pm("房屋预警导入"),Q.pm("COMPLETED")
                        ,Q.pm(lockedHouseResultSet.getTimestamp("LOCKED_TIME")),Q.pm(lockedHouseResultSet.getTimestamp("LOCKED_TIME"))
                        ,"0",Q.pm("func.limit.create")
                        ,"true",Q.pm("business")
                )+ ");");

                //work_operator
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseId.getId()),Q.pm("TASK")
                        ,Q.pm(lockedHouseResultSet.getString("EMP_CODE")),Q.pm(lockedHouseResultSet.getString("EMP_NAME"))
                        ,Q.pm(lockedHouseResultSet.getString("ID")),Q.pm(lockedHouseResultSet.getTimestamp("LOCKED_TIME"))
                )+ ");");

                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT work_task (task_id, message, task_name, pass) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Q.pm(lockedHouseResultSet.getString("ID")),Q.pm("同意")
                        ,Q.pm("建立"),Q.p(true)
                )+ ");");


                //limit_business
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT limit_business (work_id, explanation, way, from_id, limit_from) value ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseId.getId()),Q.pm(lockedHouseResultSet.getString("DESCRIPTION"))
                        ,Q.pm("HOUSE"),Long.toString(buildId.getId())
                        ,Q.pm("MANUAL")
                )+ ");");

                //house_freeze_business
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT house_freeze_business (limit_id, work_id, target_id) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseId.getId()),Long.toString(lockedHouseId.getId())
                        ,Long.toString(houseId.getId())
                )+ ");");

                //sale_limit 房屋预警 不写楼幢，楼幢预警不写房屋
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT sale_limit (limit_id, house_id,  type, status, version, " +
                        "created_at, explanation, date_to, limit_begin, work_id) value ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseId.getId()),Long.toString(houseId.getId())
                        ,Q.pm(FindWorkBook.getLockedHouseType(lockedHouseResultSet.getString("TYPE"))),Q.pm("VALID"),"0"
                        ,Q.pm(lockedHouseResultSet.getTimestamp("LOCKED_TIME")),Q.pm(lockedHouseResultSet.getString("DESCRIPTION"))
                        ,Q.pm("2123-01-01:08:00:00"),Q.pm(lockedHouseResultSet.getTimestamp("LOCKED_TIME"))
                        ,Long.toString(lockedHouseId.getId())
                )+ ");");

                //project_business
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                        "developer_name, work_type,business_id,before_info_id) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseId.getId()),Long.toString(projectId.getId())
                        ,Q.pm(UNIFIED_ID),Long.toString(projectId.getId())
                        ,Q.pm(developName),Q.pm("REFER")
                        ,Long.toString(lockedHouseId.getId()),Long.toString(projectId.getId())
                )+ ");");

                //build_business
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseId.getId()),Long.toString(buildId.getId())
                        ,Long.toString(projectId.getId()),Q.pm(lockedHouseResultSet.getTimestamp("LOCKED_TIME"))
                        ,Long.toString(buildId.getId()),Q.pm("REFER")
                        ,Long.toString(lockedHouseId.getId()),Long.toString(buildId.getId())
                )+ ");");

                //house_business
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(lockedHouseId.getId()),Long.toString(houseId.getId())
                        ,Long.toString(buildId.getId()),Q.pm(lockedHouseResultSet.getTimestamp("LOCKED_TIME"))
                        ,Long.toString(houseId.getId()),Long.toString(lockedHouseId.getId())
                        ,Q.pm("BUSINESS"),Long.toString(houseId.getId())
                )+ ");");



                limitBusinessWriter.flush();
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }



        }catch (Exception e){
            System.out.println("id is errer-----id:"+lockedHouseResultSet.getString("ID"));
            e.printStackTrace();
            return;

        }finally {
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
            if(lockedHouseResultSet!=null){
                lockedHouseResultSet.close();
            }
            if(lockedHouseStatement!=null){
                lockedHouseStatement.close();
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
