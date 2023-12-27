package houseData.recordBuild;

import com.bean.*;
import com.mapper.*;
import com.utils.FindWorkBook;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import com.utils.Q;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionException;

import javax.swing.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class projectBuildBusinessMain2 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/projectBusinessRecord2.sql";
    private static File projectBusinessFile;
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

        projectBusinessFile = new File(PROJECT_FILE);
        if(projectBusinessFile.exists()){
            projectBusinessFile.delete();
        }

        try{
            projectBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(projectBusinessFile.getAbsoluteFile());
            projectBusinessWriter = new BufferedWriter(fw);
            projectBusinessWriter.write("USE record_building;");
//            projectBusinessWriter.newLine();
//            projectBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('process_project_license','预销售许可证业务导入',false,true,0,'business');");
//            projectBusinessWriter.newLine();
//            projectBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.limit.create','预警业务导入',false,true,0,'business');");

            projectBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("projectBusinessWriter 文件创建失败");
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
        OwnerRecordBuildIdMapper ownerRecordBuildIdMapper = sqlSession.getMapper(OwnerRecordBuildIdMapper.class);
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        LandEndTimeIdMapper landEndTimeIdMapper = sqlSession.getMapper(LandEndTimeIdMapper.class);
        HouseUseTypeMapper houseUseTypeMapper = sqlSession.getMapper(HouseUseTypeMapper.class);
        FloorBeginEndMapper floorBeginEndMapper = sqlSession.getMapper(FloorBeginEndMapper.class);
        OtherHouseTypeMapper otherHouseTypeMapper = sqlSession.getMapper(OtherHouseTypeMapper.class);
        BuildIdMapper buildIdMapper = sqlSession.getMapper(BuildIdMapper.class);
        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper = sqlSession.getMapper(OwnerRecordHouseIdMapper.class);
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);
        ExceptHouseIdMapper exceptHouseIdMapper = sqlSession.getMapper(ExceptHouseIdMapper.class);
        BusinessIdMapper businessIdMapper = sqlSession.getMapper(BusinessIdMapper.class);
        BizFileIdMapper bizFileIdMapper = sqlSession.getMapper(BizFileIdMapper.class);

        BusinessId businessId = null;
        ProjectId projectId = null;
        BuildId buildId =null;
        HouseId houseId = null;
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        OwnerRecordProjectId ownerRecordProjectId = null;
        OwnerRecordBuildId ownerRecordBuildId=null;
        OwnerRecordBuildId beforeId = null;
        OwnerRecordProjectId selectBizBusiness =new OwnerRecordProjectId();
        OwnerRecordHouseId ownerRecordHouseId=null;
        LandEndTimeId landEndTimeId =null;
        ExceptHouseId exceptHouseId =null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null;
        Calendar calendar = Calendar.getInstance();
        String before_info_id_build = null,oldProjectId=null,APPLY_EMP=null,EMP_NAME=null;
        String developer_info_id = null;
        int cardNmuber = 0;
        BizFileId bizFileId = null;
        try {
            projectResultSet = projectStatement.executeQuery("SELECT P.*,A.LICENSE_NUMBER,A.COMPANY_CER_CODE,D.NAME AS DNAME FROM HOUSE_INFO.PROJECT AS P " +
                    "LEFT JOIN HOUSE_INFO.DEVELOPER AS D ON P.DEVELOPERID=D.ID " +
                    "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS A ON D.ATTACH_ID=A.ID ORDER BY P.NAME");//N6477 115 1 WHERE P.ID<>'206' WHERE P.ID='115' WHERE P.ID='xhyy
            projectResultSet.last();
            int sumCount = projectResultSet.getRow(),i=0,onNumber=1;;
            System.out.println("记录总数-"+sumCount);
            projectResultSet.beforeFirst();
            while (projectResultSet.next()){
               //在码表找到对应的新ID
                projectId = projectIdMapper.selectByOldProjectId(projectResultSet.getString("ID"));
                if(projectId == null){
                    System.out.println("没有找到对应记录检查jproject_Id--:"+projectResultSet.getString("ID"));
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
                projectBusinessResultSet = projectBusinessStatement.executeQuery("SELECT P.ID AS PID,P.*,PI.*,O.*,O.ID AS OID FROM HOUSE_OWNER_RECORD.PROJECT AS P "
                        +"LEFT JOIN HOUSE_OWNER_RECORD.PROJECT_SELL_INFO AS PI ON P.ID = PI.ID LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID "
                        +"WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL') AND DEFINE_ID='WP50' "
                        +"AND P.PROJECT_CODE='"+projectResultSet.getString("ID")+"' "
                        +"ORDER BY P.NAME,O.ID,O.APPLY_TIME;");

                if(projectBusinessResultSet.next()){
                    projectBusinessResultSet.beforeFirst();
                    while (projectBusinessResultSet.next()){

                        ownerRecordProjectId = ownerRecordProjectIdMapper.selectByOldId(projectBusinessResultSet.getString("PID"));
                        if(ownerRecordProjectId == null){
                            System.out.println("没有找到对应记录检查ownerRecordProjectId--:"+projectBusinessResultSet.getString("PID"));
                            return;
                        }
                        workbookResultSet = workbookStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.MAPPING_CORP WHERE BUSINESS_ID='"+projectBusinessResultSet.getString("OID")+"'");
                        ;
                        int mapping_corp_id=3;
                        if(workbookResultSet.next()){
                            mapping_corp_id = workbookResultSet.getInt("CODE");
                        }

//                        System.out.println(mapping_name);



                        //work projectId 作为workId
                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                        projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Q.pm("OLD")
                                ,Q.pm(projectResultSet.getString("CREATE_TIME")),Q.pm(projectBusinessResultSet.getString("CREATE_TIME"))
                                ,Q.pm("预销售许可证业务导入"),Q.pm("COMPLETED")
                                ,Q.pm(projectBusinessResultSet.getString("CREATE_TIME")),Q.pm(projectBusinessResultSet.getString("CREATE_TIME"))
                                ,"0",Q.pm("process_project_license")
                                ,"true",Q.pm("business")
                        )+ ");");
                        //操作人员记录
                        String task_id_sl=null;
                        taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUSINESS_EMP WHERE BUSINESS_ID='"+projectBusinessResultSet.getString("BUSINESS")+"'");
                        if(taskOperBusinessResultSet.next()){
                            taskOperBusinessResultSet.beforeFirst();
                            while (taskOperBusinessResultSet.next()){

                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                                projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Q.pm("TASK")
                                ,Q.pm(taskOperBusinessResultSet.getString("EMP_CODE")),Q.pm(taskOperBusinessResultSet.getString("EMP_NAME"))
                                ,Q.pm(taskOperBusinessResultSet.getString("ID")),Q.pm(taskOperBusinessResultSet.getTimestamp("OPER_TIME"))
                                )+ ");");

                                if(taskOperBusinessResultSet.getString("TYPE").equals("APPLY_EMP")){
                                    EMP_NAME = taskOperBusinessResultSet.getString("EMP_NAME");
                                    task_id_sl = taskOperBusinessResultSet.getString("ID");
                                }
                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT work_task (task_id, message, task_name, pass) VALUE ");
                                projectBusinessWriter.write("(" + Q.v(Q.pm(taskOperBusinessResultSet.getString("ID")),Q.pm("同意")
                                        ,Q.pm(FindWorkBook.getEmpType(taskOperBusinessResultSet.getString("TYPE"))),Q.p(true)
                                )+ ");");

                            }
                        }

//
//                        taskOperBusinessResultSet=taskOperBusinessStatement.executeQuery("select * from BUSINESS_FILE WHERE BUSINESS_ID ='"+projectBusinessResultSet.getString("OID")+"'");
//                        if (taskOperBusinessResultSet.next()){
//                            taskOperBusinessResultSet.beforeFirst();
//                            while (taskOperBusinessResultSet.next()){
//                                bizFileId = bizFileIdMapper.selectByOldId(taskOperBusinessResultSet.getString("ID"));
//                                if (bizFileId==null){
//                                    System.out.println("没有找到对应记录检查bizFileId:"+taskOperBusinessResultSet.getString("ID"));
//                                    return;
//                                }
//                                //work.attachment
//                                projectBusinessWriter.newLine();
//                                projectBusinessWriter.write("INSERT work.attachment (id, name, must, have, work_id, version) VALUE ");
//                                projectBusinessWriter.write("(" + Q.v(Long.toString(bizFileId.getId()),Q.pm(taskOperBusinessResultSet.getString("NAME"))
//                                        ,"true","true"
//                                        ,Long.toString(ownerRecordProjectId.getId()),"0"
//                                )+ ");");
//                                workbookResultSet = workbookStatement.executeQuery("SELECT FL.FID,FL.SHA256,FL.SIZE,FL.MIME,FL.E_TAG,UF.PRI,UF.FILE_NAME FROM UPLOAD_FILE AS UF,  " +
//                                        "FILE_LINK AS FL  WHERE UF.ID=FL.OLD AND BUSINESS_FILE_ID='"+taskOperBusinessResultSet.getString("ID")+"'");
//                                if(workbookResultSet.next()){
//                                    workbookResultSet.beforeFirst();
//                                    while (workbookResultSet.next()){
//                                        //work.work_file
//                                        projectBusinessWriter.newLine();
//                                        projectBusinessWriter.write("INSERT work.work_file(fid, sha256, attach_id, size, mime, e_tag, order_num, filename, task_id) value ");
//                                        projectBusinessWriter.write("(" + Q.v(Q.pm(workbookResultSet.getString("FID")),Q.p(workbookResultSet.getString("SHA256"))
//                                                ,Long.toString(bizFileId.getId()),Q.p(workbookResultSet.getString("SIZE"))
//                                                ,Q.pm(workbookResultSet.getString("MIME")),Q.p(workbookResultSet.getString("E_TAG"))
//                                                ,Q.pm(workbookResultSet.getString("PRI")),Q.p(workbookResultSet.getString("FILE_NAME"))
//                                                ,Q.p(task_id_sl)
//                                        )+ ");");
//                                    }
//                                }
//                            }
//                        }


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

                        workbookResultSet = workbookStatement.executeQuery("SELECT * FROM LAND_END_TIME WHERE USE_TYPE='DWELLING_KEY' AND PROJECT_ID='"+projectBusinessResultSet.getString("PID")+"'");
                        if (workbookResultSet.next()){
                            //LAND_END_TIME 同一项目么有土地用途有住宅的，土地用途多余一种的，INTEGRATION.landEndTimeId 码表取得 ，直接从LAND_END_TIME读取，
                            landEndTimeResultSet = landEndTimeStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.LAND_END_TIME WHERE PROJECT_ID='"+projectBusinessResultSet.getString("PID")+"'");
                            if(landEndTimeResultSet.next()){
                                landEndTimeResultSet.beforeFirst();
                                while (landEndTimeResultSet.next()){
                                    landEndTimeId = landEndTimeIdMapper.selectByOldId(landEndTimeResultSet.getString("ID"));
                                    if (landEndTimeId==null){
                                        System.out.println("没有找到对应记录检查jlandEndTimeId:"+landEndTimeResultSet.getString("ID"));
                                        return;
                                    }
                                    projectBusinessWriter.newLine();
                                    projectBusinessWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                                    projectBusinessWriter.write("(" + Q.v(Q.pm(landEndTimeResultSet.getTimestamp("END_TIME")),Q.pm(FindWorkBook.landUseType2(landEndTimeResultSet.getString("USE_TYPE")))
                                            ,Long.toString(ownerRecordProjectId.getId())
                                            ,Long.toString(landEndTimeId.getId())
                                    )+ ");");

                                }
                            }
                        }else{
                            //land_use_type_snapshot project 只有一种土地用途的，ID 跟业务走，LAND_END_TIME 表里同一项目么有土地用途等于住宅的
                            projectBusinessWriter.newLine();
                            projectBusinessWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                            projectBusinessWriter.write("(" + Q.v(Q.pm(projectBusinessResultSet.getTimestamp("END_USE_TIME")),Q.pm(FindWorkBook.landUseType2(projectBusinessResultSet.getString("USE_TYPE")))
                                    ,Long.toString(ownerRecordProjectId.getId())
                                    ,Long.toString(ownerRecordProjectId.getId())
                            )+ ");");


                            //LAND_END_TIME 表里同一项目么有土地用途等于住宅的， 土地用途多余一种的，INTEGRATION.landEndTimeId 码表取得
                            landEndTimeResultSet = landEndTimeStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.LAND_END_TIME WHERE USE_TYPE<>'DWELLING_KEY'  AND  PROJECT_ID='"+projectBusinessResultSet.getString("PID")+"'");
                            if(landEndTimeResultSet.next()){
                                landEndTimeResultSet.beforeFirst();
                                while (landEndTimeResultSet.next()){
                                    landEndTimeId = landEndTimeIdMapper.selectByOldId(landEndTimeResultSet.getString("ID"));
                                    if (landEndTimeId==null){
                                        System.out.println("没有找到对应记录检查jlandEndTimeId:"+landEndTimeResultSet.getString("ID"));
                                        return;
                                    }
                                    projectBusinessWriter.newLine();
                                    projectBusinessWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                                    projectBusinessWriter.write("(" + Q.v(Q.pm(landEndTimeResultSet.getTimestamp("END_TIME")),Q.pm(FindWorkBook.landUseType2(landEndTimeResultSet.getString("USE_TYPE")))
                                            ,Long.toString(ownerRecordProjectId.getId())
                                            ,Long.toString(landEndTimeId.getId())
                                    )+ ");");

                                }
                            }




                        }





                        //project_construct_snapshot
                        projectBusinessWriter.newLine();
                        //project_design_license(建设工程规划许可证号) = CREATE_PREPARE_CARD_CODE
                        //construct_license(建设工程施工许可证号) = CREATE_CARD_CODE
                        //land_design_license(建设用地规划许可证号) = CREATE_LAND_CARD_CODE
                        projectBusinessWriter.write("INSERT record_building.project_construct_snapshot (build_count, size_type, project_design_license, design_license_date, construct_license, land_design_license, construct_info_id, total_area) value ");
                        projectBusinessWriter.write("(" + Q.v(Q.p(projectBusinessResultSet.getString("BUILD_COUNT")),Q.p(FindWorkBook.projectSizeType(projectBusinessResultSet.getString("BUILD_SIZE")).getId())
                                ,Q.pm(projectBusinessResultSet.getString("CREATE_PREPARE_CARD_CODE")),Q.pm(projectBusinessResultSet.getTimestamp("CREATE_TIME"))
                                ,Q.pm(projectBusinessResultSet.getString("CREATE_CARD_CODE")),Q.pm(projectBusinessResultSet.getString("CREATE_LAND_CARD_CODE"))
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
                                ,Long.toString(ownerRecordProjectId.getId()),Long.toString(ownerRecordProjectId.getId())
                        )+ ");");

                        //PROJECT
                        projectBusinessWriter.newLine();
                        //PROJECT updated_at project_info_id
                        projectBusinessWriter.write("UPDATE project SET updated_at = '" + projectBusinessResultSet.getString("CREATE_TIME")+"',status='PUBLIC" +"',project_info_id='"+ownerRecordProjectId.getId()+"' WHERE project_id='" + projectId.getId() + "';");



                        //project_business


                        //project_business.before_info_id 获取
                        before_info_id = Long.toString(projectId.getId());
                        oldProjectId = null;
                        if(projectBusinessResultSet.getString("SELECT_BUSINESS")!=null
                                &&  !projectBusinessResultSet.getString("SELECT_BUSINESS").isBlank()){
                            workbookResultSet = workbookStatement.executeQuery("SELECT O.*,P.ID AS PID FROM HOUSE_OWNER_RECORD.PROJECT AS P LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID" +
                                    " WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL','MODIFYING','RUNNING') AND DEFINE_ID='WP50'" +
                                    " AND O.ID='"+projectBusinessResultSet.getString("SELECT_BUSINESS")+"'");
                            if(workbookResultSet.next()){
                                //System.out.println(projectBusinessResultSet.getString("SELECT_BUSINESS")+"--"+workbookResultSet.getString("PID"));
                                selectBizBusiness = ownerRecordProjectIdMapper.selectByOldId(workbookResultSet.getString("PID"));

                                if(selectBizBusiness==null){
                                    before_info_id = null;
                                    System.out.println("没有找到对应记录检查ownerRecordProjectIdMapper --- selectBizBusiness:"+workbookResultSet.getString("PID"));
                                    return;
                                }else {
                                    before_info_id = Long.toString(selectBizBusiness.getId());
                                    oldProjectId = workbookResultSet.getString("PID");

                                }
                            }
                        }

                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                                "developer_name, work_type,business_id,before_info_id,updated_at,developer_info_id) VALUE ");
                        projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Long.toString(projectId.getId())
                                ,Q.pm(UNIFIED_ID),Long.toString(ownerRecordProjectId.getId())
                                ,Q.pm(developName),Q.pm("REFER")
                                ,Long.toString(ownerRecordProjectId.getId()),before_info_id
                                ,Q.pm(projectBusinessResultSet.getTimestamp("CREATE_TIME"))
                                ,developer_info_id
                        )+ ");");
                        boolean valid = false;
                        if(projectBusinessResultSet.getString("STATUS")!=null &&
                                !projectBusinessResultSet.getString("STATUS").equals("")){
                            String o_status=projectBusinessResultSet.getString("STATUS");
                            if(o_status.equals("COMPLETE")){
                                valid =true;
                            }else{
                                valid =false;
                            }
                        }
                        //project_license_business
                        projectBusinessWriter.newLine();
                        projectBusinessWriter.write("INSERT project_license_business (work_id, sell_object, version, valid) value ");
                        projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Q.pm("INSIDE")
                                ,"0",Boolean.toString(valid)
                        )+ ");");
                        int year = 0;
                        cardNmuber=0;

                        workbookResultSet = workbookStatement.executeQuery("SELECT MA.*,PC.*,P.*,CAST(SUBSTRING(MA.NUMBER, 5) AS SIGNED) AS on_number,NUMBER FROM HOUSE_OWNER_RECORD.PROJECT_CARD AS PC LEFT JOIN HOUSE_OWNER_RECORD.MAKE_CARD AS MA ON PC.ID = MA.ID " +
                                "LEFT  JOIN  PROJECT AS P ON PC.PROJECT= P.ID WHERE MA.TYPE='PROJECT_RSHIP' AND P.ID='"+projectBusinessResultSet.getString("PID")+"' ORDER BY PRINT_TIME" );
                        if(workbookResultSet.next()){
                            if (workbookResultSet.getString("NUMBER")!=null
                                    && !workbookResultSet.getString("NUMBER").isBlank()){
                                year = FindWorkBook.getYearFromDate(workbookResultSet.getTimestamp("PRINT_TIME"));
                                cardNmuber = workbookResultSet.getInt("on_number");

                            }
                            projectBusinessWriter.newLine();
                            projectBusinessWriter.write("INSERT project_sell_license (license_id, status, project_id, year_number, " +
                                    "on_number, sell_object, make_department, word_number, build_count, house_count, house_area, house_use_area,updated_at,created_at) value ");
                            projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Q.pm(FindWorkBook.getCardStatus(projectBusinessResultSet.getString("STATUS")))
                                    ,Long.toString(projectId.getId()),Integer.toString(year)
                                    ,Integer.toString(cardNmuber),Q.pm("INSIDE")
                                    ,Q.pm(projectBusinessResultSet.getString("GOV_NAME")),Q.pm("东港字")
                                    ,projectBusinessResultSet.getString("BUILD_COUNT"),projectBusinessResultSet.getString("HOUSE_COUNT")
                                    ,Q.pm(projectBusinessResultSet.getBigDecimal("AREA")) ,Q.pm(projectBusinessResultSet.getBigDecimal("AREA"))
                                    ,Q.pm(workbookResultSet.getTimestamp("PRINT_TIME")),Q.pm(workbookResultSet.getTimestamp("PRINT_TIME"))
                            )+ ");");

                        }



                        projectBusinessWriter.flush();
                        //record_BUILD ownerRecordBuildId 作为ID
                        buildBusinessResultSet = buildBusinessStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUILD AS OB WHERE OB.PROJECT='"+projectBusinessResultSet.getString("PID")+"'");

                        if(buildBusinessResultSet.next()){
                            buildBusinessResultSet.beforeFirst();
                            while (buildBusinessResultSet.next()){
                                ownerRecordBuildId = ownerRecordBuildIdMapper.selectByOldBuildId(buildBusinessResultSet.getString("ID"));
                                //System.out.println(buildBusinessResultSet.getString("ID")+"----:--"+ownerRecordBuildId.getId());
                                if(ownerRecordBuildId==null){
                                    System.out.println("没有找到对应记录检查ownerRecordBuildId:"+buildBusinessResultSet.getString("ID"));
                                    return;
                                }
                                buildId = buildIdMapper.selectByOldBuildId(buildBusinessResultSet.getString("BUILD_CODE"));
                                if(buildId==null){
                                    System.out.println("没有找到对应记录检查buildId--ownerRecordBuildId:--:"+buildBusinessResultSet.getString("BUILD_CODE"));
                                    return;
                                }

                                String mapping_name="";
                                if(buildBusinessResultSet.getString("MAP_NUMBER")!=null && !buildBusinessResultSet.getString("MAP_NUMBER").equals("")){
                                    mapping_name=buildBusinessResultSet.getString("MAP_NUMBER")+".";
                                }
                                if(buildBusinessResultSet.getString("BLOCK_NO")!=null && !buildBusinessResultSet.getString("BLOCK_NO").equals("")){
                                    mapping_name=mapping_name+buildBusinessResultSet.getString("BLOCK_NO")+".";
                                }
                                if(buildBusinessResultSet.getString("BUILD_NO")!=null && !buildBusinessResultSet.getString("BUILD_NO").equals("")){
                                    mapping_name=mapping_name+buildBusinessResultSet.getString("BUILD_NO");
                                }

                                //build_construct_snapshot
                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT build_construct_snapshot (CONSTRUCT_INFO_ID, STRUCTURE, STRUCTURE_KEY,BUILD_ORDER, TYPE, FLOOR_COUNT, FLOOR_DOWN) VALUE ");
                                projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordBuildId.getId()),Q.pm(FindWorkBook.structure(buildBusinessResultSet.getString("STRUCTURE")).getValue())
                                        ,FindWorkBook.structure(buildBusinessResultSet.getString("STRUCTURE")).getId(),Q.pm(buildBusinessResultSet.getString("BUILD_DEVELOPER_NUMBER"))
                                        ,Q.pm(FindWorkBook.buildType(buildBusinessResultSet.getString("BUILD_TYPE")).getId()),Integer.toString(buildBusinessResultSet.getInt("UP_FLOOR_COUNT"))
                                        ,Integer.toString(buildBusinessResultSet.getInt("DOWN_FLOOR_COUNT"))
                                )+ ");");

                                //build_location_snapshot
                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT build_location_snapshot(LOCATION_INFO_ID, MAP_ID, BLOCK_ID, BUILD_NUMBER, DOOR_NUMBER) VALUE ");
                                projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordBuildId.getId()),Q.p(buildBusinessResultSet.getString("MAP_NUMBER"))
                                        ,Q.p(buildBusinessResultSet.getString("BLOCK_NO")),Q.pm(buildBusinessResultSet.getString("BUILD_NO"))
                                        ,Q.pm(buildBusinessResultSet.getString("DOOR_NO"))
                                )+ ");");


                                //project_complete_snapshot
                                String complete_info_id = null,buildStatus="SALE";

                                if(projectBusinessResultSet.getString("TYPE")!=null &&
                                        !projectBusinessResultSet.getString("TYPE").equals("")
                                        && projectBusinessResultSet.getString("TYPE").equals("NOW_SELL")){
                                    complete_info_id = Long.toString(ownerRecordBuildId.getId());
                                    buildStatus = "COMPLETED_SALE";
                                    int complete_year =FindWorkBook.getYearFromDate(projectBusinessResultSet.getTimestamp("CREATE_TIME"));

                                    projectBusinessWriter.newLine();
                                    projectBusinessWriter.write("INSERT project_complete_snapshot (complete_info_id , license, complete_year, record_number, record_time, record_gov) VALUE ");
                                    projectBusinessWriter.write("(" + Q.v(complete_info_id,Q.p("未知")
                                            ,Integer.toString(complete_year),Q.pm("未知")
                                            ,Q.pm(projectBusinessResultSet.getTimestamp("CREATE_TIME")),Q.pm("未知")
                                    )+ ");");

                                }


                                //build_snapshot
                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT build_snapshot (build_info_id, location_info_id, construct_info_id, " +
                                        "build_id, work_id,complete_info_id) VALUE ");
                                projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordBuildId.getId()),Long.toString(ownerRecordBuildId.getId())
                                        ,Long.toString(ownerRecordBuildId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(ownerRecordProjectId.getId()),complete_info_id

                                )+ ");");


                                //BUILD
                                projectBusinessWriter.newLine();
                                //PROJECT updated_at project_info_id
                                projectBusinessWriter.write("update build set updated_at = '" + projectBusinessResultSet.getString("CREATE_TIME")
                                        +"',mapping_name='"+mapping_name +"',status='"+buildStatus+"',build_info_id='"+ownerRecordBuildId.getId()+"' WHERE build_id='" + buildId.getId() + "';");

                                //project_builds_snapshot
                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT project_builds_snapshot (project_info_id, build_info_id) VALUE");
                                projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Long.toString(ownerRecordBuildId.getId())
                                )+ ");");






                                //build_business
                                before_info_id_build = Long.toString(buildId.getId());//默认
                                workbookResultSet = workbookStatement.executeQuery("SELECT HB.* FROM HOUSE_OWNER_RECORD.PROJECT AS P LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID " +
                                        "LEFT JOIN BUILD AS HB ON P.ID= HB.PROJECT " +
                                        "WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL','MODIFYING') AND DEFINE_ID='WP50' " +
                                        " AND O.ID='"+projectBusinessResultSet.getString("SELECT_BUSINESS")+"'"+
                                        "AND P.ID='"+oldProjectId+"' AND BUILD_CODE='"+buildBusinessResultSet.getString("BUILD_CODE")+"'");

                                if(workbookResultSet.next()) {
                                    if (workbookResultSet.getString("ID") != null &&
                                            !workbookResultSet.getString("ID").equals("")) {
                                        beforeId = ownerRecordBuildIdMapper.selectByOldBuildId(workbookResultSet.getString("ID"));

                                        if (beforeId == null) {
                                            System.out.println("没有找到对应beforeBuildId记录检查:--BUILD_CODE:" + buildBusinessResultSet.getString("BUILD_CODE") + "---oldProjectId:" + oldProjectId);
                                            return;
                                        } else {
                                            before_info_id_build = Long.toString(beforeId.getId());
                                        }
                                    }
                                }

//                                System.out.println("id--:"+ownerRecordBuildId.getId()+"--before_info_id_build--"+before_info_id_build);
                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                                projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(projectId.getId()),Q.pm(projectBusinessResultSet.getTimestamp("CREATE_TIME"))
                                        ,Long.toString(ownerRecordBuildId.getId()),Q.pm("BUSINESS")
                                        ,Long.toString(ownerRecordBuildId.getId()),Q.p(before_info_id_build)
                                )+ ");");


                                //project_license_builds license_id=work_id=ownerRecordProjectId
                                projectBusinessWriter.newLine();
                                projectBusinessWriter.write("INSERT project_license_builds (license_id, build_id) value ");
                                projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Long.toString(buildId.getId())
                                )+ ");");

                                //build_use_type_total_snapshot USEtYPE = 'other'  判断不出来具体，不倒了，遇到时在处理
                                workbookResultSet = workbookStatement.executeQuery("select * from SELL_TYPE_TOTAL where BUILD_ID='"+buildBusinessResultSet.getString("ID")+"' and AREA>0 and USE_TYPE<>'OTHER'");
                                if(workbookResultSet.next()){
                                    workbookResultSet.beforeFirst();
                                    while (workbookResultSet.next()){
                                        projectBusinessWriter.newLine();
                                        projectBusinessWriter.write("INSERT build_use_type_total_snapshot (count, area, use_area, house_type, build_id, work_id) VALUE ");
                                        projectBusinessWriter.write("(" + Q.v(Q.pm(workbookResultSet.getString("COUNT")),Q.pm(workbookResultSet.getBigDecimal("AREA"))
                                                ,Q.pm(workbookResultSet.getBigDecimal("AREA")),Q.pm(FindWorkBook.changeLandSnapshot(workbookResultSet.getString("USE_TYPE")).getId())
                                                ,Long.toString(buildId.getId()),Long.toString(ownerRecordProjectId.getId())
                                        )+ ");");
                                    }
                                }



                                workbookResultSet = workbookStatement.executeQuery("select * from EXCEPT_HOUSE WHERE BUILD ='"+buildBusinessResultSet.getString("ID")+"'");
                                //license_limit_snapshot
                                if(workbookResultSet.next()){
                                    //limit_business
                                    projectBusinessWriter.newLine();
                                    projectBusinessWriter.write("INSERT limit_business (work_id, explanation, way, from_id, limit_from) value ");
                                    projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Q.pm("预售不可售设置")
                                            ,Q.pm("HOUSE"),Long.toString(buildId.getId())
                                            ,Q.pm("LICENSE")
                                    )+ ");");
                                    workbookResultSet.beforeFirst();
                                    while (workbookResultSet.next()){
                                        houseId = houseIdMapper.selectByOldHouseId(workbookResultSet.getString("HOUSE_CODE"));
                                        if(houseId==null){
                                            System.out.println("license_limit_snapshot没有找到对应HOUSE_CODE记录检查:--:"+workbookResultSet.getString("HOUSE_CODE"));
                                            return;
                                        }
                                        exceptHouseId = exceptHouseIdMapper.selectByOldHouseId(workbookResultSet.getString("ID"));
                                        if(exceptHouseId == null){
                                            System.out.println("没有找到对应exceptHouseId记录检查:--:"+workbookResultSet.getString("ID"));
                                            return;
                                        }

                                        //预售许可证时 不可售房屋临时记录表，通过limit_business 建立业务表，记录业务信息，最终记录到 sale_limit
                                        projectBusinessWriter.newLine();
                                        projectBusinessWriter.write("INSERT license_limit_snapshot (business_id, work_id, build_id, house_id, house_info_id) VALUE ");
                                        projectBusinessWriter.write("(" + Q.v(Long.toString(exceptHouseId.getId()),Long.toString(ownerRecordProjectId.getId())
                                                ,Long.toString(buildId.getId()),Long.toString(houseId.getId())
                                                ,Long.toString(houseId.getId())
                                        )+ ");");

//

                                        //house_freeze_business
                                        projectBusinessWriter.newLine();
                                        projectBusinessWriter.write("INSERT house_freeze_business (limit_id, work_id, target_id) VALUE ");
                                        projectBusinessWriter.write("(" + Q.v(Long.toString(exceptHouseId.getId()),Long.toString(ownerRecordProjectId.getId())
                                                ,Long.toString(houseId.getId())
                                        )+ ");");

                                        //sale_limit 房屋预警 不写楼幢，楼幢预警不写房屋
                                        projectBusinessWriter.newLine();
                                        projectBusinessWriter.write("INSERT sale_limit (limit_id, house_id,  type, status, version, " +
                                                "created_at, explanation, date_to, limit_begin, work_id) value ");
                                        projectBusinessWriter.write("(" + Q.v(Long.toString(exceptHouseId.getId()),Long.toString(houseId.getId())
                                                ,Q.pm("FREEZE"),Q.pm("VALID"),"0"
                                                ,Q.pm(projectResultSet.getString("CREATE_TIME")),Q.pm("预售不可售")
                                                ,Q.pm("2123-01-01:08:00:00"),Q.pm(projectBusinessResultSet.getTimestamp("CREATE_TIME"))
                                                ,Long.toString(ownerRecordProjectId.getId())
                                        )+ ");");

                                        projectBusinessWriter.newLine();
                                        projectBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                                        projectBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordProjectId.getId()),Long.toString(houseId.getId())
                                                ,Long.toString(buildId.getId()),Q.pm(projectBusinessResultSet.getTimestamp("CREATE_TIME"))
                                                ,Long.toString(houseId.getId()),Long.toString(exceptHouseId.getId())
                                                ,Q.pm("BUSINESS"),Long.toString(houseId.getId())
                                        )+ ");");

                                    }
                                }
                                projectBusinessWriter.flush();
                            }
                        }
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
