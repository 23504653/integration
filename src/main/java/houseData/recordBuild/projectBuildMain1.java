package houseData.recordBuild;

import com.bean.BusinessId;
import com.bean.JointCorpDevelop;
import com.bean.LandEndTimeId;
import com.bean.ProjectId;
import com.mapper.BusinessIdMapper;
import com.mapper.JointCorpDevelopMapper;
import com.mapper.LandEndTimeIdMapper;
import com.mapper.ProjectIdMapper;
import com.utils.*;
import org.apache.ibatis.session.SqlSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class projectBuildMain1 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static final String PROJECT_ERROR_FILE="/projectError1.sql";
    private static final String PROJECT_FILE="/projectRecord1.sql";
    private static BufferedWriter projectWriterError;
    private static BufferedWriter projectWriter;
    private static File projectFileError;
    private static File projectFile;
    private static Statement projectStatement;
    private static Statement businessStatement;
    private static ResultSet projectResultSet;
    private static ResultSet businessResultSet;
    private static Statement landEndTimeStatement;
    private static ResultSet landEndTimeResultSet;
    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;


    public static void main(String agr[]) throws SQLException {

        projectFileError = new File(PROJECT_ERROR_FILE);
        if(projectFileError.exists()){
            projectFileError.delete();
        }
        projectFile = new File(PROJECT_FILE);
        if(projectFile.exists()){
            projectFile.delete();
        }

        try{
            projectFile.createNewFile();
            FileWriter fw = new FileWriter(projectFile.getAbsoluteFile());
            projectWriter = new BufferedWriter(fw);
            projectWriter.write("USE record_building;");
            projectWriter.newLine();
            projectWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('process_project_import','预销售许可证业务导入',false,true,0,'data');");
            projectWriter.newLine();
            projectWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) "
                    +"value (1,'OLD','2023-10-28 18:30:45','2023-10-28 18:30:45','预销售许可证业务导入','COMPLETED','2023-10-28 18:30:45','2023-10-28 18:30:45',0,'process_project_import',false,'data');");
            projectWriter.newLine();
            projectWriter.write("INSERT work_operator (work_id, type, user_id, user_name, org_name, task_id) VALUE (1,'CREATE','0','root','预销售许可证业务导入','wxy_project');");
            projectWriter.newLine();
            projectWriter.write("INSERT work_task (TASK_ID, MESSAGE, TASK_NAME, PASS) VALUE ('wxy_project','预销售许可证业务导入','预销售许可证业务导入',true);");
            projectWriter.flush();
        }catch (IOException e){
            System.out.println("projectWriter 文件创建失败");
            e.printStackTrace();
            return;
        }


        try{
            projectFileError.createNewFile();
            FileWriter fw = new FileWriter(projectFileError.getAbsoluteFile());
            projectWriterError = new BufferedWriter(fw);
            projectWriterError.write("project--错误记录:");
            projectWriterError.newLine();
            projectWriterError.flush();
        }catch (IOException e){
            System.out.println("projectWriterError 文件创建失败");
            e.printStackTrace();
            return;
        }
        projectStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        businessStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        landEndTimeStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        workbookStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);
        LandEndTimeIdMapper landEndTimeIdMapper = sqlSession.getMapper(LandEndTimeIdMapper.class);
        BusinessIdMapper businessIdMapper = sqlSession.getMapper(BusinessIdMapper.class);
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        ProjectId projectId = null;
        LandEndTimeId landEndTimeId= null;
        BusinessId businessId = new BusinessId();
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        String developName=null,UNIFIED_ID=null,districtCode=null;

        try {
//          projectResultSet = projectStatement.executeQuery("SELECT * FROM HOUSE_INFO.PROJECT ORDER BY NAME");
            projectResultSet = projectStatement.executeQuery("SELECT P.*,A.LICENSE_NUMBER,D.NAME AS DNAME FROM HOUSE_INFO.PROJECT AS P " +
                    "LEFT JOIN HOUSE_INFO.DEVELOPER AS D ON P.DEVELOPERID=D.ID " +
                    "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS A ON D.ATTACH_ID=A.ID WHERE P.ID='1' ORDER BY P.NAME");
            projectResultSet.last();
            int sumCount = projectResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            projectResultSet.beforeFirst();
            while (projectResultSet.next()){

                //在码表找到对应的新ID
                 projectId = projectIdMapper.selectByOldProjectId(projectResultSet.getString("ID"));
                if(projectId == null){
                    projectWriterError.newLine();
                    projectWriterError.write("没有找到对应记录检查jproject_Id--:"+projectResultSet.getString("ID"));
                    projectWriterError.flush();
                    System.out.println("没有找到对应记录检查jproject_Id--:"+projectResultSet.getString("ID"));
                    return;
                }
                //获取开发新ID=UNIFIED_ID 营业执照号，没有的用开发商新ID替代 没有开发商用未知开发商
                if(projectResultSet.getString("DEVELOPERID")!=null && projectResultSet.getString("DEVELOPERID").isBlank()){
                    jointCorpDevelop = jointCorpDevelopMapper.selectByDevelopId(projectResultSet.getString("DEVELOPERID"));
                    UNIFIED_ID = projectResultSet.getString("LICENSE_NUMBER");
                    if (UNIFIED_ID==null || UNIFIED_ID.isBlank()){
                        UNIFIED_ID = Long.toString(jointCorpDevelop.getCorpId());
                        developName = jointCorpDevelop.getName();
                    }
                }else{
                    UNIFIED_ID ="0";
                    developName = "未知";
                }


                businessResultSet = businessStatement.executeQuery("SELECT P.ID AS PID,P.*,PI.*,O.* FROM HOUSE_OWNER_RECORD.PROJECT AS P "
                +"LEFT JOIN HOUSE_OWNER_RECORD.PROJECT_SELL_INFO AS PI ON P.ID = PI.ID LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID "
                +"WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL','MODIFYING','RUNNING') AND DEFINE_ID='WP50' "
                +"AND P.PROJECT_CODE='"+projectResultSet.getString("ID")+"' "
                +"ORDER BY P.NAME,O.ID,O.APPLY_TIME;");

                if(businessResultSet.next()){//项目办理过预售业务的  HOUSE_INFO.PROJECT.ID =HOUSE_OWNER_RECORD.PROJECT.PROJECT_CODE
                    businessResultSet.beforeFirst();
                    while (businessResultSet.next()) {
                        businessId = businessIdMapper.selectByOldBusinessId(businessResultSet.getString("BUSINESS"));;
                        if (businessId ==null){
                            projectWriterError.newLine();
                            projectWriterError.write("没有找到对应记录检查businessId:"+businessResultSet.getString("ID"));
                            projectWriterError.flush();
                            System.out.println("没有找到对应记录检查businessId:"+businessResultSet.getString("ID"));
                            return;
                        }
                        //land_snapshot land_info_id 取businessId
                        projectWriter.newLine();
                        projectWriter.write("INSERT record_building.land_snapshot (CAPARCEL_NUMBER, LAND_NUMBER, PROPERTY,BEGIN_DATE, TAKE_TYPE_KEY, TAKE_TYPE, AREA, ADDRESS, LICENSE_NUMBER, LICENSE_TYPE, LICENSE_TYPE_KEY, LAND_INFO_ID) VALUE ");
                        projectWriter.write("(" + Q.v(Q.pm("未知"),Q.pm(businessResultSet.getString("NUMBER"))
                                ,Q.p(FindWorkBook.changeLandProperty(businessResultSet.getString("LAND_PROPERTY")).getValue()),Q.pm(businessResultSet.getTimestamp("BEGIN_USE_TIME"))
                                ,Q.pm(FindWorkBook.changeLandTakeType(businessResultSet.getString("LAND_GET_MODE")).getId()),Q.p(FindWorkBook.changeLandTakeType(businessResultSet.getString("LAND_GET_MODE")).getValue())
                                ,Q.pm(businessResultSet.getBigDecimal("LAND_AREA")),Q.pm(businessResultSet.getString("LAND_ADDRESS"))
                                ,Q.pm(businessResultSet.getString("LAND_CARD_NO")),Q.pm(FindWorkBook.changeLandCardType(businessResultSet.getString("LAND_CARD_TYPE")).getValue())
                                ,Q.pm(FindWorkBook.changeLandCardType(businessResultSet.getString("LAND_CARD_TYPE")).getId())
                                ,Long.toString(businessId.getId())
                        )+ ");");
                        projectWriter.flush();

                        //land_use_type_snapshot project 只有一种土地用途的，ID 跟业务走，多余的在
                        projectWriter.newLine();
                        projectWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                        projectWriter.write("(" + Q.v(Q.pm(businessResultSet.getTimestamp("END_USE_TIME")),Q.pm(FindWorkBook.landUseType(businessResultSet.getString("USE_TYPE")))
                                ,Long.toString(businessId.getId())
                                ,Long.toString(businessId.getId())
                        )+ ");");
                        projectWriter.flush();

                        //LAND_END_TIME 土地用途多余一种的，INTEGRATION.landEndTimeId 码表取得
                        landEndTimeResultSet = landEndTimeStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.LAND_END_TIME WHERE PROJECT_ID='"+businessResultSet.getString("PID")+"'");
                        if(landEndTimeResultSet.next()){
                            landEndTimeResultSet.beforeFirst();
                            while (landEndTimeResultSet.next()){
                                landEndTimeId = landEndTimeIdMapper.selectByOldId(landEndTimeResultSet.getString("ID"));
                                if(landEndTimeId!=null && landEndTimeId.getOid()!=null && !landEndTimeId.getOid().equals("")){
                                    projectWriter.newLine();
                                    projectWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                                    projectWriter.write("(" + Q.v(Q.pm(landEndTimeResultSet.getTimestamp("END_TIME")),Q.pm(landEndTimeResultSet.getString("USE_TYPE"))
                                            ,Long.toString(businessId.getId())
                                            ,Long.toString(landEndTimeId.getId())
                                    )+ ");");
                                    projectWriter.flush();
                                }else {
                                    projectWriterError.newLine();
                                    projectWriterError.write("没有找到对应记录检查jlandEndTimeId:"+landEndTimeResultSet.getString("ID"));
                                    projectWriterError.flush();
                                    System.out.println("没有找到对应记录检查jlandEndTimeId:"+landEndTimeResultSet.getString("ID"));
                                    return;
                                }
                            }
                        }
                        //project_construct_snapshot
                        projectWriter.newLine();
                        projectWriter.write("INSERT record_building.project_construct_snapshot (build_count, size_type, project_design_license, design_license_date, construct_license, land_design_license, construct_info_id, total_area) value ");
                        projectWriter.write("(" + Q.v(Q.p(businessResultSet.getString("BUILD_COUNT")),Q.p(businessResultSet.getString("BUILD_SIZE"))
                                ,Q.pm(businessResultSet.getString("CREATE_PREPARE_CARD_CODE")),Q.pm(businessResultSet.getTimestamp("CREATE_TIME"))
                                ,Q.pm(businessResultSet.getString("CREATE_CARD_CODE")),Q.pm(businessResultSet.getString("CREATE_PREPARE_CARD_CODE"))
                                ,Long.toString(businessId.getId()),Q.pm(businessResultSet.getBigDecimal("AREA"))
                        )+ ");");
                        projectWriter.flush();
                    }
                }else{//项目没办理过预售业务的 project_snapshot 写空记录 相关表写对应的空记录 用projectId 做个表主键ID
                    //land_snapshot land_info_id
                    businessId.setId(projectId.getId());
                    projectWriter.newLine();
                    projectWriter.write("INSERT record_building.land_snapshot (CAPARCEL_NUMBER, LAND_NUMBER, BEGIN_DATE, " +
                            "TAKE_TYPE_KEY, TAKE_TYPE, AREA, ADDRESS, LICENSE_NUMBER, LICENSE_TYPE, LICENSE_TYPE_KEY, LAND_INFO_ID) VALUE ");
                    projectWriter.write("(" + Q.v(Q.pm("未知"),Q.pm("未知")
                            ,Q.pm("1801-01-01:08:00:00"),"6"
                            ,Q.pm("其它"),"0"
                            ,Q.pm("未知"),Q.pm("未知")
                            ,Q.pm("国有土地使用证"),"2"
                            ,Long.toString(projectId.getId())
                    )+ ");");
                    projectWriter.flush();

                    //land_use_type_snapshot
                    projectWriter.newLine();
                    projectWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                    projectWriter.write("(" + Q.v(Q.pm("1801-01-01:08:00:00"),Q.pm("DWELLING")
                            ,Long.toString(projectId.getId())
                            ,Long.toString(projectId.getId())
                    )+ ");");
                    projectWriter.flush();

                    //project_construct_snapshot
                    projectWriter.newLine();
                    projectWriter.write("INSERT record_building.project_construct_snapshot (project_design_license, design_license_date, construct_license, " +
                            "construct_info_id, total_area) value ");
                    projectWriter.write("(" + Q.v(Q.pm("未知"),Q.pm("1980-01-01 00:00:00")
                            ,Q.pm("未知")
                            ,Long.toString(projectId.getId()),"0"
                    )+ ");");
                    projectWriter.flush();
                }

                //project_base_snapshot
                workbookResultSet = workbookStatement.executeQuery("SELECT S.DISTRICT FROM HOUSE_INFO.PROJECT AS P LEFT JOIN HOUSE_INFO.SECTION AS S " +
                        "ON P.SECTIONID=S.ID " +
                        "WHERE P.ID='"+projectResultSet.getString("ID")+"'");
                if(workbookResultSet.next()){
                    projectWriter.newLine();
                    districtCode = workbookResultSet.getString("DISTRICT");
                    projectWriter.write("INSERT record_building.project_base_snapshot (base_info_id, project_name, district_code) value ");
                    projectWriter.write("(" + Q.v(Long.toString(businessId.getId())
                            ,Q.pm(projectResultSet.getString("NAME"))
                            ,Q.pm(districtCode)
                    )+ ");");
                    projectWriter.flush();
                }else {
                    System.out.println("项目id--："+projectResultSet.getString("ID")+ "--没有对应的DISTRICT");
                    return;
                }


               //project_snapshot
                projectWriter.newLine();
                projectWriter.write("INSERT record_building.project_snapshot (project_info_id, project_id, base_info_id, construct_info_id, land_info_id, work_id) VALUE ");
                projectWriter.write("(" + Q.v(Long.toString(businessId.getId()),Long.toString(projectId.getId())
                        ,Long.toString(businessId.getId()),Long.toString(businessId.getId())
                        ,Long.toString(businessId.getId()),"1"
                )+ ");");
                projectWriter.flush();

                //project
                projectWriter.newLine(); // 建立一个未知开发商将没有开发商的项目都挂上去 project_business 没写
                projectWriter.write("INSERT record_building.project (project_id, developer_id, developer_name, project_info_id, status, " +
                        "version, address, project_name, district_code, pin) value ");
                projectWriter.write("(" + Q.v(Long.toString(projectId.getId()),UNIFIED_ID
                        ,Q.pm(developName),Long.toString(businessId.getId())
                        ,Q.p("PUBLIC"),"0"
                        ,Q.pm(projectResultSet.getString("ADDRESS")),Q.pm(projectResultSet.getString("NAME"))
                        ,Q.pm(districtCode),Q.p(PinyinTools.getPinyinCode(projectResultSet.getString("NAME")))
                )+ ");");
                projectWriter.flush();

                //project_business








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

            if (businessResultSet!=null){
                businessResultSet.close();
            }
            if(businessStatement!=null){
                businessStatement.close();
            }
            MyConnection.closeConnection();
        }

    }
}
