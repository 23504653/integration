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

public class hosueDel15 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String Limit_FILE="/hosueDelyj.sql";
    private static File limitBusinessFile;
    private static BufferedWriter limitBusinessWriter;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    private static Statement buildStatement;
    private static ResultSet buildResultSet;

    private static Statement lockedHouseStatement;
    private static ResultSet lockedHouseResultSet;

    private static Statement projectStatement;
    private static ResultSet projectResultSet;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;
//旧系统中 测绘删除的房屋加预警
    public static void main(String agr[]) throws SQLException {
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
        SqlSession sqlSession = MybatisUtils.getSqlSession();

        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);
        BuildIdMapper buildIdMapper = sqlSession.getMapper(BuildIdMapper.class);
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper = sqlSession.getMapper(OwnerRecordHouseIdMapper.class);

        lockedHouseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();

        BuildId buildId =null;
        HouseId houseId = null;
        ProjectId projectId = null;
        String developer_info_id = null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null;
        long id=8500000;
        try {
            lockedHouseResultSet = lockedHouseStatement.executeQuery("select * from HOUSE_INFO.HOUSE WHERE DELETED=1");
            lockedHouseResultSet.last();
            int sumCount = lockedHouseResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            lockedHouseResultSet.beforeFirst();
            while (lockedHouseResultSet.next()){
                houseId = houseIdMapper.selectByOldHouseId(lockedHouseResultSet.getString("ID"));
                if(houseId==null){
                    System.out.println("hosueDel15没有找到对应HOUSE_CODE记录检查:--:"+lockedHouseResultSet.getString("ID"));
                    return;
                }
                houseResultSet = houseStatement.executeQuery("SELECT * FROM HOUSE_INFO.HOUSE WHERE ID='"+lockedHouseResultSet.getString("ID")+"'");
                houseResultSet.next();
                buildId = buildIdMapper.selectByOldBuildId(houseResultSet.getString("BuildID"));
                if(buildId==null){
                    System.out.println("hosueDel15没有找到对应记录检查BuildID-:"+buildResultSet.getString("BuildID"));
                    return;
                }
                buildResultSet = buildStatement.executeQuery("SELECT * FROM HOUSE_INFO.BUILD WHERE ID='"+houseResultSet.getString("BuildID")+"'");
                buildResultSet.next();

                projectResultSet = projectStatement.executeQuery("SELECT P.*,A.LICENSE_NUMBER,A.COMPANY_CER_CODE,D.NAME AS DNAME FROM HOUSE_INFO.PROJECT AS P " +
                        "LEFT JOIN HOUSE_INFO.DEVELOPER AS D ON P.DEVELOPERID=D.ID " +
                        "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS A ON D.ATTACH_ID=A.ID WHERE P.ID ='"+buildResultSet.getString("PROJECT_ID")+"'  ORDER BY P.NAME");

                projectResultSet.next();
                projectId = projectIdMapper.selectByOldProjectId(projectResultSet.getString("ID"));
                if(projectId == null){
                    System.out.println("hosueDel15没有找到对应记录检查PROJECT_ID--:"+buildResultSet.getString("PROJECT_ID"));
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
                developer_info_id = null;
                if(projectResultSet.getString("COMPANY_CER_CODE")!=null &&
                        !projectResultSet.getString("COMPANY_CER_CODE").equals("")){
                    developer_info_id = Long.toString(jointCorpDevelop.getCorpId());

                }

                //work lockedHouseId 作为workId
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(id),Q.pm("OLD")
                        ,Q.pm(Q.nowFormatTime()),Q.pm(Q.nowFormatTime())
                        ,Q.pm("原系统测绘回收站房屋建立预警"),Q.pm("COMPLETED")
                        ,Q.pm(Q.nowFormatTime()),Q.pm(Q.nowFormatTime())
                        ,"0",Q.pm("func.limit.create")
                        ,"true",Q.pm("action")
                )+ ");");

                //work_operator
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(id),Q.pm("TASK")
                        ,Q.pm("ROOT"),Q.pm("root")
                        ,Long.toString(id),Q.pm(Q.nowFormatTime())
                )+ ");");

                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT work_task (task_id, message, task_name, pass) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(id),Q.pm("预警建立")
                        ,Q.pm("建立"),Q.p(true)
                )+ ");");

                //limit_business
                String explanation = "原系统测绘删除的房屋建立预警!房屋编号:"+lockedHouseResultSet.getString("ID");
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT limit_business (work_id, explanation, way, from_id, limit_from) value ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(id),Q.pm(explanation)
                        ,Q.pm("HOUSE"),Long.toString(buildId.getId())
                        ,Q.pm("MANUAL")
                )+ ");");

                //house_freeze_business
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT house_freeze_business (limit_id, work_id, target_id) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(id),Long.toString(id)
                        ,Long.toString(houseId.getId())
                )+ ");");
                //sale_limit 房屋预警 不写楼幢，楼幢预警不写房屋
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT sale_limit (limit_id, house_id,  type, status, version, " +
                        "created_at, explanation, date_to, limit_begin, work_id) value ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(id),Long.toString(houseId.getId())
                        ,Q.pm("FREEZE"),Q.pm("VALID"),"0"
                        ,Q.pm(Q.nowFormatTime()),Q.pm("{\"explanation\":\""+explanation+"\"}")
                        ,"null",Q.pm(Q.nowFormatTime())
                        ,Long.toString(id)
                )+ ");");

                //project_business
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                        "developer_name, work_type,business_id,before_info_id,developer_info_id,updated_at) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(id),Long.toString(projectId.getId())
                        ,Q.pm(UNIFIED_ID),Long.toString(projectId.getId())
                        ,Q.pm(developName),Q.pm("REFER")
                        ,Long.toString(houseId.getId()),Long.toString(projectId.getId())
                        ,developer_info_id,Q.pm(Q.nowFormatTime())
                )+ ");");

               //build_business
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(id),Long.toString(buildId.getId())
                        ,Long.toString(projectId.getId()),Q.pm(Q.nowFormatTime())
                        ,Long.toString(buildId.getId()),Q.pm("REFER")
                        ,Long.toString(id),Long.toString(buildId.getId())
                )+ ");");

                //house_business
                limitBusinessWriter.newLine();
                limitBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                limitBusinessWriter.write("(" + Q.v(Long.toString(id),Long.toString(houseId.getId())
                        ,Long.toString(buildId.getId()),Q.pm(Q.nowFormatTime())
                        ,Long.toString(houseId.getId()),Long.toString(id)
                        ,Q.pm("BUSINESS"),Long.toString(houseId.getId())
                )+ ");");





                limitBusinessWriter.flush();
                i++;
                id++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {

        }

    }
}
