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

public class limitOwnerBusinessMain6 {

    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String Limit_ERROR_FILE="/limitOwnerBusinessError6.sql";
    private static final String Limit_FILE="/limitOwnerBusinessRecord6.sql";
    private static File limitOwnerBusinessFileError;
    private static File limitOwnerBusinessFile;
    private static BufferedWriter limitOwnerBusinessWriterError;
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
        houseRecordStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        try {
            houseRecordResultSet = houseRecordStatement.executeQuery("select * from HOUSE_OWNER_RECORD.HOUSE_RECORD as HR where HR.HOUSE_STATUS='OWNERED' AND HR.HOUSE_CODE ='0021-0'");
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

                houseId = houseIdMapper.selectByOldHouseId(houseRecordResultSet.getString("HOUSE_CODE"));
                if(houseId==null){
                    System.out.println("limitOwnerBusinessMain6没有找到对应HOUSE_CODE记录检查:--:"+houseRecordResultSet.getString("HOUSE_CODE"));
                    return;
                }

                houseResultSet = houseStatement.executeQuery("SELECT * FROM HOUSE_INFO.HOUSE WHERE ID='"+houseRecordResultSet.getString("HOUSE_CODE")+"'");
                houseResultSet.next();

                buildId = buildIdMapper.selectByOldBuildId(houseResultSet.getString("BuildID"));
                if(buildId==null){
                    System.out.println("limitOwnerBusinessMain6没有找到对应记录检查BuildID-:"+buildResultSet.getString("BuildID"));
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
                    System.out.println("limitOwnerBusinessMain6没有找到对应记录检查PROJECT_ID--:"+buildResultSet.getString("PROJECT_ID"));
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
                limitOwnerBusinessWriter.newLine();
                limitOwnerBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                limitOwnerBusinessWriter.write("(" + Q.v(Long.toString(houseRecordId.getId()),Q.pm("OLD")
                        ,Q.pm(Q.nowFormatTime()),Q.pm(Q.nowFormatTime())
                        ,Q.pm("已办产权建立预警"),Q.pm("COMPLETED")
                        ,Q.pm(Q.nowFormatTime()),Q.pm(Q.nowFormatTime())
                        ,"0",Q.pm("func.limit.import")
                        ,"true",Q.pm("business")
                )+ ");");
                //work_operator
                limitOwnerBusinessWriter.newLine();
                limitOwnerBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                limitOwnerBusinessWriter.write("(" + Q.v(Long.toString(houseRecordId.getId()),Q.pm("TASK")
                        ,Q.pm("root"),Q.pm("管理员")
                        ,Q.pm("root"),Q.pm(Q.nowFormatTime())
                )+ ");");

                limitOwnerBusinessWriter.newLine();
                limitOwnerBusinessWriter.write("INSERT work_task (task_id, message, task_name, pass) VALUE ");
                limitOwnerBusinessWriter.write("(" + Q.v(Long.toString(houseRecordId.getId()),Q.pm("同意")
                        ,Q.pm("建立"),Q.p(true)
                )+ ");");

                //limit_business
                limitOwnerBusinessWriter.newLine();
                limitOwnerBusinessWriter.write("INSERT limit_business (work_id, explanation, way, from_id, limit_from) value ");
                limitOwnerBusinessWriter.write("(" + Q.v(Long.toString(houseRecordId.getId()),Q.pm("由管理员将原系统已办产权的房屋建立预警！ 原房屋编号："+houseRecordResultSet.getString("HOUSE_CODE"))
                        ,Q.pm("HOUSE"),Long.toString(buildId.getId())
                        ,Q.pm("MANUAL")
                )+ ");");

                //house_freeze_business
                limitOwnerBusinessWriter.newLine();
                limitOwnerBusinessWriter.write("INSERT house_freeze_business (limit_id, work_id, target_id) VALUE ");
                limitOwnerBusinessWriter.write("(" + Q.v(Long.toString(houseRecordId.getId()),Long.toString(houseRecordId.getId())
                        ,Long.toString(houseId.getId())
                )+ ");");

                //sale_limit 房屋预警 不写楼幢，楼幢预警不写房屋
                limitOwnerBusinessWriter.newLine();
                limitOwnerBusinessWriter.write("INSERT sale_limit (limit_id, house_id,  type, status, version, " +
                        "created_at, explanation, date_to, limit_begin, work_id) value ");
                limitOwnerBusinessWriter.write("(" + Q.v(Long.toString(houseRecordId.getId()),Long.toString(houseId.getId())
                        ,Q.pm("FREEZE"),Q.pm("VALID"),"0"
                        ,Q.pm(Q.nowFormatTime()),Q.pm("由管理员将原系统已办产权的房屋建立预警！ 原房屋编号："+houseRecordResultSet.getString("HOUSE_CODE"))
                        ,Q.pm("2123-01-01:08:00:00"),Q.pm(Q.nowFormatTime())
                        ,Long.toString(houseRecordId.getId())
                )+ ");");

                //project_business
                limitOwnerBusinessWriter.newLine();
                limitOwnerBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                        "developer_name, work_type,business_id,before_info_id) VALUE ");
                limitOwnerBusinessWriter.write("(" + Q.v(Long.toString(houseRecordId.getId()),Long.toString(projectId.getId())
                        ,Q.pm(UNIFIED_ID),Long.toString(projectId.getId())
                        ,Q.pm(developName),Q.pm("REFER")
                        ,Long.toString(houseRecordId.getId()),Long.toString(projectId.getId())
                )+ ");");

                //build_business
                limitOwnerBusinessWriter.newLine();
                limitOwnerBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                limitOwnerBusinessWriter.write("(" + Q.v(Long.toString(houseRecordId.getId()),Long.toString(buildId.getId())
                        ,Long.toString(projectId.getId()),Q.pm(Q.nowFormatTime())
                        ,Long.toString(buildId.getId()),Q.pm("REFER")
                        ,Long.toString(houseRecordId.getId()),Long.toString(buildId.getId())
                )+ ");");


                //house_business
                limitOwnerBusinessWriter.newLine();
                limitOwnerBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                limitOwnerBusinessWriter.write("(" + Q.v(Long.toString(houseRecordId.getId()),Long.toString(houseId.getId())
                        ,Long.toString(buildId.getId()),Q.pm(Q.nowFormatTime())
                        ,Long.toString(houseId.getId()),Long.toString(houseRecordId.getId())
                        ,Q.pm("BUSINESS"),Long.toString(houseId.getId())
                )+ ");");

                limitOwnerBusinessWriter.flush();
                i++;
                System.out.println(i + "/" + String.valueOf(sumCount));
            }
        }catch (Exception e){
            System.out.println("limitOwnerBusinessMain6 id is errer-----HOUSE_OCDE:"+houseRecordResultSet.getString("HOUSE_CODE"));
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
