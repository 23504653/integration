package houseData.recordBuild;

import com.bean.JointCorpDevelop;
import com.bean.LandEndTimeId;
import com.bean.OwnerRecordProjectId;
import com.bean.ProjectId;
import com.mapper.*;
import com.utils.FindWorkBook;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import com.utils.Q;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class projectBuildBusinessMain {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_ERROR_FILE="/projectBusinessError1.sql";
    private static final String PROJECT_FILE="/projectBusinessRecord1.sql";
    private static File projectBusinessFileError;
    private static File projectBusinessFile;
    private static BufferedWriter projectBusinessWriterError;
    private static BufferedWriter projectBusinessWriter;
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




    public static void main(String agr[]) throws SQLException {

        projectBusinessFileError = new File(PROJECT_ERROR_FILE);
        if(projectBusinessFileError.exists()){
            projectBusinessFileError.delete();
        }
        projectBusinessFile = new File(PROJECT_FILE);
        if(projectBusinessFile.exists()){
            projectBusinessFile.delete();
        }

        try{
            projectBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(projectBusinessFile.getAbsoluteFile());
            projectBusinessWriter = new BufferedWriter(fw);
            projectBusinessWriter.write("USE record_building;");
            projectBusinessWriter.newLine();
            projectBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('process_project_import','预销售许可证业务导入',false,true,0,'data');");
            projectBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("projectBusinessWriter 文件创建失败");
            e.printStackTrace();
            return;
        }

        try{
            projectBusinessFileError.createNewFile();
            FileWriter fw = new FileWriter(projectBusinessFileError.getAbsoluteFile());
            projectBusinessWriterError = new BufferedWriter(fw);
            projectBusinessWriterError.write("project--错误记录:");
            projectBusinessWriterError.newLine();
            projectBusinessWriterError.flush();
        }catch (IOException e){
            System.out.println("projectWriterError 文件创建失败");
            e.printStackTrace();
            return;
        }

        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        landEndTimeStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);
        OwnerRecordProjectIdMapper ownerRecordProjectIdMapper = sqlSession.getMapper(OwnerRecordProjectIdMapper.class);
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        LandEndTimeIdMapper landEndTimeIdMapper = sqlSession.getMapper(LandEndTimeIdMapper.class);
        HouseUseTypeMapper houseUseTypeMapper = sqlSession.getMapper(HouseUseTypeMapper.class);
        FloorBeginEndMapper floorBeginEndMapper = sqlSession.getMapper(FloorBeginEndMapper.class);
        OtherHouseTypeMapper otherHouseTypeMapper = sqlSession.getMapper(OtherHouseTypeMapper.class);

        ProjectId projectId = null;
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        OwnerRecordProjectId ownerRecordProjectId = null;
        OwnerRecordProjectId selectBizBusiness =new OwnerRecordProjectId();
        LandEndTimeId landEndTimeId =null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null;

        try {
            projectResultSet = projectStatement.executeQuery("SELECT P.*,A.LICENSE_NUMBER,D.NAME AS DNAME FROM HOUSE_INFO.PROJECT AS P " +
                    "LEFT JOIN HOUSE_INFO.DEVELOPER AS D ON P.DEVELOPERID=D.ID " +
                    "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS A ON D.ATTACH_ID=A.ID WHERE P.ID='N6477' ORDER BY P.NAME");//N6477
            projectResultSet.last();
            int sumCount = projectResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            projectResultSet.beforeFirst();
            while (projectResultSet.next()){
               //在码表找到对应的新ID
                projectId = projectIdMapper.selectByOldProjectId(projectResultSet.getString("ID"));
                if(projectId == null){
                    projectBusinessWriterError.newLine();
                    projectBusinessWriterError.write("没有找到对应记录检查jproject_Id--:"+projectResultSet.getString("ID"));
                    projectBusinessWriterError.flush();
                    System.out.println("没有找到对应记录检查jproject_Id--:"+projectResultSet.getString("ID"));
                    return;
                }
                projectBusinessResultSet = projectBusinessStatement.executeQuery("SELECT P.ID AS PID,P.*,PI.*,O.* FROM HOUSE_OWNER_RECORD.PROJECT AS P "
                        +"LEFT JOIN HOUSE_OWNER_RECORD.PROJECT_SELL_INFO AS PI ON P.ID = PI.ID LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID "
                        +"WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL','MODIFYING') AND DEFINE_ID='WP50' "
                        +"AND P.PROJECT_CODE='"+projectResultSet.getString("ID")+"' "
                        +"ORDER BY P.NAME,O.ID,O.APPLY_TIME desc;");

                if(projectBusinessResultSet.next()){
                    projectBusinessResultSet.beforeFirst();
                    while (projectBusinessResultSet.next()){

                        ownerRecordProjectId = ownerRecordProjectIdMapper.selectByOldId(projectBusinessResultSet.getString("PID"));
                        if(ownerRecordProjectId == null){
                            projectBusinessWriterError.newLine();
                            projectBusinessWriterError.write("没有找到对应记录检查ownerRecordProjectId--:"+projectBusinessResultSet.getString("PID"));
                            projectBusinessWriterError.flush();
                            System.out.println("没有找到对应记录检查ownerRecordProjectId--:"+projectBusinessResultSet.getString("PID"));
                            return;
                        }

                        //work projectId 作为workId
                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                        projectBusinessWriter.write("(" + Q.v(Q.pm(Long.toString(ownerRecordProjectId.getId())),Q.pm("OLD")
                                ,Q.pm(projectResultSet.getString("CREATE_TIME")),Q.pm(projectResultSet.getString("CREATE_TIME"))
                                ,Q.pm("预销售许可证业务导入"),Q.pm("COMPLETED")
                                ,Q.pm(projectResultSet.getString("CREATE_TIME")),Q.pm(projectResultSet.getString("CREATE_TIME"))
                                ,"0",Q.pm("process_project_import")
                                ,"true",Q.pm("data")
                        )+ ");");
                        //操作人员记录
                        taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUSINESS_EMP WHERE BUSINESS_ID='"+projectBusinessResultSet.getString("BUSINESS")+"'");
                        if(taskOperBusinessResultSet.next()){
                            taskOperBusinessResultSet.beforeFirst();
                            while (taskOperBusinessResultSet.next()){

                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                                projectBusinessWriter.write("(" + Q.v(Q.pm(Long.toString(ownerRecordProjectId.getId())),Q.pm("TASK")
                                ,Q.pm(taskOperBusinessResultSet.getString("EMP_CODE")),Q.pm(taskOperBusinessResultSet.getString("EMP_NAME"))
                                ,Q.pm(taskOperBusinessResultSet.getString("ID")),Q.pm(taskOperBusinessResultSet.getTimestamp("OPER_TIME"))
                                )+ ");");

                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT work_task (task_id, message, task_name, pass) VALUE ");
                                projectBusinessWriter.write("(" + Q.v(Q.pm(taskOperBusinessResultSet.getString("ID")),Q.pm("同意")
                                        ,Q.pm(taskOperBusinessResultSet.getString("TYPE")),Q.p(true)
                                )+ ");");

                            }
                        }


                        //land_snapshot land_info_id 取HOUSE_OWNER_RECORD.PROJECT.ID
                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("INSERT record_building.land_snapshot (CAPARCEL_NUMBER, LAND_NUMBER, PROPERTY,BEGIN_DATE, TAKE_TYPE_KEY, TAKE_TYPE, AREA, ADDRESS, LICENSE_NUMBER, LICENSE_TYPE, LICENSE_TYPE_KEY, LAND_INFO_ID) VALUE ");
                        projectBusinessWriter.write("(" + Q.v(Q.pm("未知"),Q.pm(projectBusinessResultSet.getString("NUMBER"))
                                ,Q.p(FindWorkBook.changeLandProperty(projectBusinessResultSet.getString("LAND_PROPERTY")).getValue()),Q.pm(projectBusinessResultSet.getTimestamp("BEGIN_USE_TIME"))
                                ,Q.pm(FindWorkBook.changeLandTakeType(projectBusinessResultSet.getString("LAND_GET_MODE")).getId()),Q.p(FindWorkBook.changeLandTakeType(projectBusinessResultSet.getString("LAND_GET_MODE")).getValue())
                                ,Q.pm(projectBusinessResultSet.getBigDecimal("LAND_AREA")),Q.pm(projectBusinessResultSet.getString("LAND_ADDRESS"))
                                ,Q.pm(projectBusinessResultSet.getString("LAND_CARD_NO")),Q.pm(FindWorkBook.changeLandCardType(projectBusinessResultSet.getString("LAND_CARD_TYPE")).getValue())
                                ,Q.pm(FindWorkBook.changeLandCardType(projectBusinessResultSet.getString("LAND_CARD_TYPE")).getId())
                                ,Long.toString(ownerRecordProjectId.getId())
                        )+ ");");

                        //land_use_type_snapshot project 只有一种土地用途的，ID 跟业务走，
                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                        projectBusinessWriter.write("(" + Q.v(Q.pm(projectBusinessResultSet.getTimestamp("END_USE_TIME")),Q.pm(FindWorkBook.landUseType(projectBusinessResultSet.getString("USE_TYPE")))
                                ,Long.toString(ownerRecordProjectId.getId())
                                ,Long.toString(ownerRecordProjectId.getId())
                        )+ ");");


                        //LAND_END_TIME 土地用途多余一种的，INTEGRATION.landEndTimeId 码表取得
                        landEndTimeResultSet = landEndTimeStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.LAND_END_TIME WHERE PROJECT_ID='"+projectBusinessResultSet.getString("PID")+"'");
                        if(landEndTimeResultSet.next()){
                            landEndTimeResultSet.beforeFirst();
                            while (landEndTimeResultSet.next()){
                                landEndTimeId = landEndTimeIdMapper.selectByOldId(landEndTimeResultSet.getString("ID"));
                                if (landEndTimeId==null){
                                    projectBusinessWriterError.newLine();
                                    projectBusinessWriterError.write("没有找到对应记录检查jlandEndTimeId:"+landEndTimeResultSet.getString("ID"));
                                    projectBusinessWriterError.flush();
                                    System.out.println("没有找到对应记录检查jlandEndTimeId:"+landEndTimeResultSet.getString("ID"));
                                    return;
                                }
                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                                projectBusinessWriter.write("(" + Q.v(Q.pm(landEndTimeResultSet.getTimestamp("END_TIME")),Q.pm(landEndTimeResultSet.getString("USE_TYPE"))
                                       ,Long.toString(ownerRecordProjectId.getId())
                                       ,Long.toString(landEndTimeId.getId())
                                )+ ");");

                            }
                        }

                        //project_construct_snapshot
                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("INSERT record_building.project_construct_snapshot (build_count, size_type, project_design_license, design_license_date, construct_license, land_design_license, construct_info_id, total_area) value ");
                        projectBusinessWriter.write("(" + Q.v(Q.p(projectBusinessResultSet.getString("BUILD_COUNT")),Q.p(FindWorkBook.projectSizeType(projectBusinessResultSet.getString("BUILD_SIZE")).getId())
                                ,Q.pm(projectBusinessResultSet.getString("CREATE_PREPARE_CARD_CODE")),Q.pm(projectBusinessResultSet.getTimestamp("CREATE_TIME"))
                                ,Q.pm(projectBusinessResultSet.getString("CREATE_CARD_CODE")),Q.pm(projectBusinessResultSet.getString("CREATE_PREPARE_CARD_CODE"))
                                ,Long.toString(ownerRecordProjectId.getId()),Q.pm(projectBusinessResultSet.getBigDecimal("AREA"))
                        )+ ");");


                        //project_base_snapshot
                        workbookResultSet = workbookStatement.executeQuery("SELECT S.DISTRICT FROM HOUSE_INFO.PROJECT AS P LEFT JOIN HOUSE_INFO.SECTION AS S " +
                                "ON P.SECTIONID=S.ID " +
                                "WHERE P.ID='"+projectResultSet.getString("ID")+"'");
                        if(workbookResultSet.next()){
                            projectBusinessWriter.newLine();
                            districtCode = workbookResultSet.getString("DISTRICT");
                            projectBusinessWriter.write("INSERT record_building.project_base_snapshot (base_info_id, project_name, district_code) value ");
                            projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId())
                                    ,Q.pm(projectResultSet.getString("NAME"))
                                    ,Q.pm(districtCode)
                            )+ ");");
                        }else {
                            System.out.println("项目id--："+projectResultSet.getString("ID")+ "--没有对应的DISTRICT");
                            return;
                        }

                        //project_snapshot
                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("INSERT record_building.project_snapshot (project_info_id, project_id, base_info_id, construct_info_id, land_info_id, work_id) VALUE ");
                        projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Long.toString(projectId.getId())
                                ,Long.toString(ownerRecordProjectId.getId()),Long.toString(ownerRecordProjectId.getId())
                                ,Long.toString(ownerRecordProjectId.getId()),"1"
                        )+ ");");

                        //PROJECT
                        projectBusinessWriter.newLine();
                        //PROJECT updated_at project_info_id
                        projectBusinessWriter.write("UPDATE project SET updated_at = '" + projectBusinessResultSet.getTimestamp("CREATE_TIME") +"',project_info_id='"+ownerRecordProjectId.getId()+"' WHERE project_id='" + projectId.getId() + "';");


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
                        //project_business


                        //work_business.before_info_id 获取
                        if(projectBusinessResultSet.getString("SELECT_BUSINESS")!=null
                                &&  !projectBusinessResultSet.getString("SELECT_BUSINESS").isBlank()){
                            System.out.println("33333--"+projectBusinessResultSet.getString("SELECT_BUSINESS"));
                            workbookResultSet = workbookStatement.executeQuery("SELECT O.*,P.ID AS PID FROM HOUSE_OWNER_RECORD.PROJECT AS P LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID" +
                                    " WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL','MODIFYING','RUNNING') AND DEFINE_ID='WP50'" +
                                    " AND O.ID='"+projectBusinessResultSet.getString("SELECT_BUSINESS")+"'");

                            System.out.println("1111--"+projectBusinessResultSet.getString("SELECT_BUSINESS"));

                            if(workbookResultSet.next()){
                                selectBizBusiness = ownerRecordProjectIdMapper.selectByOldId(workbookResultSet.getString("PID"));
                                System.out.println("2222--"+workbookResultSet.getString("PID"));
                                if(selectBizBusiness==null){
                                    System.out.println("没有找到对应记录检查ownerRecordProjectIdMapper --- selectBizBusiness:"+workbookResultSet.getString("PID"));
                                    return;
                                }else {
                                    before_info_id = Long.toString(selectBizBusiness.getId());
                                }

                            }else{
                              System.out.println("NOT_FIND_HOUSE_OWNER_RECORD.PROJECT.ID:--"+projectBusinessResultSet.getString("PID")+"--SELECT_BUSINESS--"+projectBusinessResultSet.getString("SELECT_BUSINESS"));
                              return;
                            }
                        }else {
                            before_info_id = null;
                        }

                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                                "developer_name, work_type,business_id,before_info_id) VALUE ");
                        projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Long.toString(projectId.getId())
                                ,UNIFIED_ID,Long.toString(ownerRecordProjectId.getId())
                                ,Q.pm(developName),Q.pm("BUSINESS")
                                ,Long.toString(ownerRecordProjectId.getId()),Q.p(before_info_id)
                        )+ ");");

                        projectBusinessWriter.flush();

                    }
                }







                projectBusinessWriter.flush();
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));

            }

        }catch (Exception e){
            System.out.println("id is errer-----id:"+projectResultSet.getString("ID"));
            e.printStackTrace();
            return;
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
