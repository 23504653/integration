package houseData.recordBuild;


import com.bean.*;
import com.mapper.*;
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
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_ERROR_FILE="/projectError1.sql";
    private static final String PROJECT_FILE="/projectRecord1.sql";
    private static BufferedWriter projectWriterError;
    private static BufferedWriter projectWriter;
    private static File projectFileError;
    private static File projectFile;
    private static Statement projectStatement;
    private static ResultSet projectResultSet;
    private static Statement buildStatement;
    private static ResultSet buildResultSet;
    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    /**
     * 导入空间库信息
     * @param agr
     * @throws SQLException
     */

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
            projectWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.building.project.import','导入工程项目',false,true,0,'data');");
            projectWriter.newLine();
            projectWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.building.build.import','导入楼幢',false,true,0,'data');");
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
        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        ProjectIdMapper projectIdMapper =  sqlSession.getMapper(ProjectIdMapper.class);
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        BuildIdMapper buildIdMapper = sqlSession.getMapper(BuildIdMapper.class);
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);
        HouseUseTypeMapper houseUseTypeMapper = sqlSession.getMapper(HouseUseTypeMapper.class);
        FloorBeginEndMapper floorBeginEndMapper = sqlSession.getMapper(FloorBeginEndMapper.class);
        OtherHouseTypeMapper otherHouseTypeMapper = sqlSession.getMapper(OtherHouseTypeMapper.class);

        ProjectId projectId = null;
        BuildId buildId =null;
        HouseId houseId = null;
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        HouseUseType houseUseType= new HouseUseType();
        OtherHouseType otherHouseType = null;
        FloorBeginEnd floorBeginEnd = new FloorBeginEnd();

        String developName=null,UNIFIED_ID=null,districtCode=null;



        try {
            projectResultSet = projectStatement.executeQuery("SELECT P.*,A.LICENSE_NUMBER,D.NAME AS DNAME FROM HOUSE_INFO.PROJECT AS P " +
                    "LEFT JOIN HOUSE_INFO.DEVELOPER AS D ON P.DEVELOPERID=D.ID " +
                    "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS A ON D.ATTACH_ID=A.ID WHERE P.ID='1' ORDER BY P.NAME");//N6477
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
                //work projectId 作为workId
                projectWriter.newLine();
                projectWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                projectWriter.write("(" + Q.v(Q.pm(Long.toString(projectId.getId())),Q.pm("OLD")
                        ,Q.pm(projectResultSet.getString("CREATE_TIME")),Q.pm(projectResultSet.getString("CREATE_TIME"))
                        ,Q.pm("导入工程项目"),Q.pm("COMPLETED")
                        ,Q.pm(projectResultSet.getString("CREATE_TIME")),Q.pm(projectResultSet.getString("CREATE_TIME"))
                        ,"0",Q.pm("func.building.project.import")
                        ,"true",Q.pm("data")
                )+ ");");
               //work_operator projectId 作为task_id
                projectWriter.newLine();
                projectWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id) VALUE ");
                projectWriter.write("(" + Q.v(Q.pm(Long.toString(projectId.getId())),Q.pm("CREATE")
                        ,"0",Q.pm("root"),Q.pm(Long.toString(projectId.getId()))
                )+ ");");

                //land_snapshot
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
                //
                projectWriter.newLine();
                projectWriter.write("INSERT record_building.land_use_type_snapshot (end_date, use_type, land_info_id, id) value ");
                projectWriter.write("(" + Q.v(Q.pm("1801-01-01:08:00:00"),Q.pm("DWELLING")
                        ,Long.toString(projectId.getId())
                        ,Long.toString(projectId.getId())
                )+ ");");

                //project_construct_snapshot
                projectWriter.newLine();
                projectWriter.write("INSERT record_building.project_construct_snapshot (project_design_license, design_license_date, construct_license, " +
                        "construct_info_id, total_area) value ");
                projectWriter.write("(" + Q.v(Q.pm("未知"),Q.pm("1980-01-01 00:00:00")
                        ,Q.pm("未知")
                        ,Long.toString(projectId.getId()),"0"
                )+ ");");

                //project_base_snapshot
                workbookResultSet = workbookStatement.executeQuery("SELECT S.DISTRICT FROM HOUSE_INFO.PROJECT AS P LEFT JOIN HOUSE_INFO.SECTION AS S " +
                        "ON P.SECTIONID=S.ID " +
                        "WHERE P.ID='"+projectResultSet.getString("ID")+"'");
                if(workbookResultSet.next()){
                    projectWriter.newLine();
                    districtCode = workbookResultSet.getString("DISTRICT");
                    projectWriter.write("INSERT record_building.project_base_snapshot (base_info_id, project_name, district_code) value ");
                    projectWriter.write("(" + Q.v(Long.toString(projectId.getId())
                            ,Q.pm(projectResultSet.getString("NAME"))
                            ,Q.pm(districtCode)
                    )+ ");");
                }else {
                    System.out.println("项目id--："+projectResultSet.getString("ID")+ "--没有对应的DISTRICT");
                    return;
                }
                //project_snapshot
                projectWriter.newLine();
                projectWriter.write("INSERT record_building.project_snapshot (project_info_id, project_id, base_info_id, construct_info_id, land_info_id, work_id) VALUE ");
                projectWriter.write("(" + Q.v(Long.toString(projectId.getId()),Long.toString(projectId.getId())
                        ,Long.toString(projectId.getId()),Long.toString(projectId.getId())
                        ,Long.toString(projectId.getId()),"1"
                )+ ");");

                projectWriter.newLine(); // 建立一个未知开发商将没有开发商的项目都挂上去
                projectWriter.write("INSERT record_building.project (project_id, developer_id,updated_at, developer_name, project_info_id, status, " +
                        "version, address, project_name, district_code, pin) value ");
                projectWriter.write("(" + Q.v(Long.toString(projectId.getId()),UNIFIED_ID,Q.pm(projectResultSet.getString("CREATE_TIME"))
                        ,Q.pm(developName),Long.toString(projectId.getId())
                        ,Q.p("PUBLIC"),"0"
                        ,Q.pm(projectResultSet.getString("ADDRESS")),Q.pm(projectResultSet.getString("NAME"))
                        ,Q.pm(districtCode),Q.p(PinyinTools.getPinyinCode(projectResultSet.getString("NAME")))
                )+ ");");
                //project_business
                projectWriter.newLine();
                projectWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                        "developer_name, work_type,business_id) VALUE ");
                projectWriter.write("(" + Q.v(Long.toString(projectId.getId()),Long.toString(projectId.getId())
                        ,UNIFIED_ID,Long.toString(projectId.getId())
                        ,Q.pm(developName),Q.pm("CREATE")
                        ,Long.toString(projectId.getId())
                )+ ");");
                projectWriter.flush();
                //HOUSE_INFO.BUILD.ID 作为build表主键
                buildResultSet = buildStatement.executeQuery("SELECT * FROM HOUSE_INFO.BUILD AS B WHERE B.PROJECT_ID ='"+projectResultSet.getString("ID") +"'");

                if(buildResultSet.next()){
                     buildResultSet.beforeFirst();
                    while (buildResultSet.next()){
                        buildId = buildIdMapper.selectByOldBuildId(buildResultSet.getString("ID"));
                        if(buildId==null){
                            projectWriterError.newLine();
                            projectWriterError.write("没有找到对应记录检查buildId:"+buildResultSet.getString("ID"));
                            projectWriterError.flush();
                            System.out.println("没有找到对应记录检查buildId:"+buildResultSet.getString("ID"));
                            return;
                        }

                        //work buildId 作为workId

                        projectWriter.newLine();
                        projectWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                        projectWriter.write("(" + Q.v(Q.pm(Long.toString(buildId.getId())),Q.pm("OLD")
                                ,Q.pm(buildResultSet.getString("MAP_TIME")),Q.pm(buildResultSet.getString("MAP_TIME"))
                                ,Q.pm("导入楼幢"),Q.pm("COMPLETED")
                                ,Q.pm(buildResultSet.getString("MAP_TIME")),Q.pm(buildResultSet.getString("MAP_TIME"))
                                ,"0",Q.pm("func.building.build.import")
                                ,"true",Q.pm("data")
                        )+ ");");
                        //work_operator projectId 作为task_id
                        projectWriter.newLine();
                        projectWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id) VALUE ");
                        projectWriter.write("(" + Q.v(Q.pm(Long.toString(buildId.getId())),Q.pm("CREATE")
                                ,"0",Q.pm("root"),Q.pm(Long.toString(buildId.getId()))
                        )+ ");");

                        //build_construct_snapshot
                        projectWriter.newLine();
                        projectWriter.write("INSERT build_construct_snapshot (CONSTRUCT_INFO_ID, STRUCTURE, STRUCTURE_KEY,BUILD_ORDER, TYPE, FLOOR_COUNT, FLOOR_DOWN) VALUE ");
                        projectWriter.write("(" + Q.v(Long.toString(buildId.getId()),Q.pm(FindWorkBook.structure(buildResultSet.getString("STRUCTURE")).getValue())
                                ,FindWorkBook.structure(buildResultSet.getString("STRUCTURE")).getId(),Q.pm(buildResultSet.getString("DEVELOPER_NUMBER"))
                                ,Q.pm(FindWorkBook.buildType(buildResultSet.getString("BUILD_TYPE")).getId()),Integer.toString(buildResultSet.getInt("UP_FLOOR_COUNT"))
                                ,Integer.toString(buildResultSet.getInt("DOWN_FLOOR_COUNT"))
                        )+ ");");

                        //build_location_snapshot
                        projectWriter.newLine();
                        projectWriter.write("INSERT build_location_snapshot(LOCATION_INFO_ID, MAP_ID, BLOCK_ID, BUILD_NUMBER, DOOR_NUMBER) VALUE ");
                        projectWriter.write("(" + Q.v(Long.toString(buildId.getId()),Q.p(buildResultSet.getString("MAP_NUMBER"))
                                ,Q.p(buildResultSet.getString("BLOCK_NO")),Q.pm(buildResultSet.getString("BUILD_NO"))
                                ,Q.pm(buildResultSet.getString("DEVELOPER_NUMBER"))
                        )+ ");");

                        //build_snapshot
                        projectWriter.newLine();
                        projectWriter.write("INSERT build_snapshot (build_info_id, location_info_id, construct_info_id, " +
                                "build_id, work_id) VALUE ");
                        projectWriter.write("(" + Q.v(Long.toString(buildId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(buildId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(buildId.getId())

                        )+ ");");
                        //BUILD_ID
                        projectWriter.newLine();
                        projectWriter.write("INSERT build (BUILD_ID, PROJECT_ID, STATUS, BUILD_INFO_ID, BUILD_NAME, VERSION, MAPPING_NAME, DOOR_NUMBER) VALUE");
                        projectWriter.write("(" + Q.v(Long.toString(buildId.getId()),Long.toString(projectId.getId())
                                ,Q.pm("SALE"),Long.toString(buildId.getId())
                                ,Q.pm(buildResultSet.getString("NAME")),"0"
                                ,Q.pm(buildResultSet.getTimestamp("MAP_TIME")),Q.pm(buildResultSet.getString("DOOR_NO"))
                        )+ ");");

                        //project_builds_snapshot
                        projectWriter.newLine();
                        projectWriter.write("INSERT project_builds_snapshot (project_info_id, build_info_id) VALUE");
                        projectWriter.write("(" + Q.v(Long.toString(projectId.getId()),Long.toString(buildId.getId())
                        )+ ");");

                        //project_business 添加一个build_business 同时 添加一条 project_business workType=REFER

                        //project_business
                        projectWriter.newLine();
                        projectWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                                "developer_name, work_type,business_id) VALUE ");
                        projectWriter.write("(" + Q.v(Long.toString(buildId.getId()),Long.toString(projectId.getId())
                                ,UNIFIED_ID,Long.toString(projectId.getId())
                                ,Q.pm(developName),Q.pm("REFER")
                                ,Long.toString(buildId.getId())
                        )+ ");");



                        //build_business
                        projectWriter.newLine();
                        projectWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id) value ");
                        projectWriter.write("(" + Q.v(Long.toString(buildId.getId()),Long.toString(buildId.getId())
                                ,Long.toString(projectId.getId()),Q.pm(Q.nowFormatTime())
                                ,Long.toString(buildId.getId()),Q.pm("CREATE")
                                ,Long.toString(buildId.getId())
                        )+ ");");



                        projectWriter.flush();


                        //house
                        houseResultSet = houseStatement.executeQuery("select * from HOUSE_INFO.HOUSE WHERE BUILDID ='"+buildResultSet.getString("ID")+"'");
                        if(houseResultSet.next()){
                            houseResultSet.beforeFirst();
                            while(houseResultSet.next()){
                                houseId = houseIdMapper.selectByOldHouseId(houseResultSet.getString("ID"));
                                if(houseId==null){
                                    projectWriterError.newLine();
                                    projectWriterError.write("没有找到对应记录检查houseID:"+houseResultSet.getString("ID"));
                                    projectWriterError.flush();
                                    System.out.println("没有找到对应记录检查houseID:"+houseResultSet.getString("ID"));
                                    return;
                                }
                                houseUseType = houseUseTypeMapper.selectByDesignUseType(houseResultSet.getString("DESIGN_USE_TYPE"));
                                if(houseUseType ==null){//房屋类型不是其它
                                    projectWriterError.newLine();
                                    projectWriterError.write("没有找到对应记录检查houseUseType:"+houseResultSet.getString("ID"));
                                    projectWriterError.flush();
                                    System.out.println("没有找到对应记录检查houseUseType:"+houseResultSet.getString("ID"));
                                    return;
                                }
                                if(houseUseType.getHouseType().equals("Null")){//DesignUseType 用途是其它 需要调 otherHouseType表 进行房屋类型的取值
                                    otherHouseType = otherHouseTypeMapper.selectByHouseId(houseResultSet.getString("ID"));
                                    if (otherHouseType == null){
                                        projectWriterError.newLine();
                                        projectWriterError.write("没有找到对应记录检查otherHouseTypeMapper:"+houseResultSet.getString("ID"));
                                        projectWriterError.flush();
                                        System.out.println("没有找到对应记录检查otherHouseTypeMapper:"+houseResultSet.getString("ID"));
                                        return;
                                    }else {
                                        houseUseType.setHouseType(otherHouseType.getHouseType());
                                    }
                                }

                                // workid,用build的workId，和defineID buildid z
                                floorBeginEnd = floorBeginEndMapper.selectByName(houseResultSet.getString("IN_FLOOR_NAME"));
                                if(floorBeginEnd==null){
                                    projectWriterError.newLine();
                                    projectWriterError.write("没有找到对应记录检查floorBeginEndMapper:"+houseResultSet.getString("ID"));
                                    projectWriterError.flush();
                                    System.out.println("没有找到对应记录检查floorBeginEndMapper:"+houseResultSet.getString("ID"));
                                    return;
                                }
                                //apartment_snapshot
                                projectWriter.newLine();
                                projectWriter.write("INSERT apartment_snapshot (apartment_info_id,layer_type, layer_type_key," +
                                        " house_type, floor_begin, floor_end, floor_name," +
                                        "unit, use_type, use_type_key, apartment_number) value ");
                                projectWriter.write("(" + Q.v(Long.toString(houseId.getId()),Q.p(FindWorkBook.structure(houseResultSet.getString("STRUCTURE")).getValue())
                                        ,Q.p(FindWorkBook.structure(houseResultSet.getString("STRUCTURE")).getId()),Q.pm(houseUseType.getHouseType())
                                        ,Integer.toString(floorBeginEnd.getBeginFloor())
                                        ,Integer.toString(floorBeginEnd.getEndFloor()),Q.pm(houseResultSet.getString("IN_FLOOR_NAME"))
                                        ,Q.pm(houseResultSet.getString("HOUSE_UNIT_NAME")),Q.pm(houseUseType.getLabel())
                                        ,Integer.toString(houseUseType.getValue()),Q.pm(houseResultSet.getString("HOUSE_ORDER"))
                                )+ ");");

                                //apartment_snapshot
                                projectWriter.newLine();
                                projectWriter.write("INSERT apartment_mapping_snapshot (mapping_info_id, area, area_use, area_share, area_loft,mapping_corp_info_id) VALUE ");
                                projectWriter.write("(" + Q.v(Long.toString(houseId.getId()),Q.pm(houseResultSet.getBigDecimal("HOUSE_AREA"))
                                        ,Q.pm(houseResultSet.getBigDecimal("USE_AREA")),Q.pm(houseResultSet.getBigDecimal("COMM_AREA"))
                                        ,Q.pm(houseResultSet.getBigDecimal("LOFT_AREA")),Q.p(FindWorkBook.getMappingCorpId(buildResultSet.getString("MAP_CORP")).getId())
                                )+ ");");

                                //house_snapshot
                                projectWriter.newLine();
                                projectWriter.write("INSERT house_snapshot (HOUSE_INFO_ID, HOUSE_ID, MAPPING_INFO_ID, APARTMENT_INFO_ID, WORK_ID, UNIT_CODE) value  ");
                                projectWriter.write("(" + Q.v(Long.toString(houseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(houseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(buildId.getId()),Q.p(houseResultSet.getString("UNIT_NUMBER"))
                                )+ ");");
                                //house
                                projectWriter.newLine();
                                projectWriter.write("INSERT house (HOUSE_ID, BUILD_ID, HOUSE_INFO_ID, STATUS, MAPPING_CORP_ID, MAPPING_CORP_NAME, VERSION) value ");
                                projectWriter.write("(" + Q.v(Long.toString(houseId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(houseId.getId()),Q.pm("SALE"),Q.p(FindWorkBook.getMappingCorpId(buildResultSet.getString("MAP_CORP")).getId())
                                        ,Q.p(FindWorkBook.getMappingCorpId(buildResultSet.getString("MAP_CORP")).getValue()),"0"
                                )+ ");");


                                projectWriter.newLine();
                                projectWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                                projectWriter.write("(" + Q.v(Q.pm(Long.toString(houseId.getId())),Q.pm("OLD")
                                        ,Q.pm(buildResultSet.getString("MAP_TIME")),Q.pm(buildResultSet.getString("MAP_TIME"))
                                        ,Q.pm("导入房屋"),Q.pm("COMPLETED")
                                        ,Q.pm(buildResultSet.getString("MAP_TIME")),Q.pm(buildResultSet.getString("MAP_TIME"))
                                        ,"0",Q.pm("func.building.build.import")
                                        ,"true",Q.pm("data")
                                )+ ");");
                                //work_operator projectId 作为task_id
                                projectWriter.newLine();
                                projectWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id) VALUE ");
                                projectWriter.write("(" + Q.v(Q.pm(Long.toString(houseId.getId())),Q.pm("CREATE")
                                        ,"0",Q.pm("root"),Q.pm(Long.toString(houseId.getId()))
                                )+ ");");

                                //project_business
                                projectWriter.newLine();
                                projectWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                                        "developer_name, work_type,business_id) VALUE ");
                                projectWriter.write("(" + Q.v(Long.toString(houseId.getId()),Long.toString(projectId.getId())
                                        ,UNIFIED_ID,Long.toString(projectId.getId())
                                        ,Q.pm(developName),Q.pm("REFER")
                                        ,Long.toString(houseId.getId())
                                )+ ");");



                                //build_business
                                projectWriter.newLine();
                                projectWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id) value ");
                                projectWriter.write("(" + Q.v(Long.toString(houseId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(projectId.getId()),Q.pm(Q.nowFormatTime())
                                        ,Long.toString(buildId.getId()),Q.pm("REFER")
                                        ,Long.toString(houseId.getId())
                                )+ ");");

                                //house_business
                                projectWriter.newLine();
                                projectWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type) VALUE ");
                                projectWriter.write("(" + Q.v(Long.toString(houseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(buildId.getId()),Q.pm(Q.nowFormatTime())
                                        ,Long.toString(houseId.getId()),Long.toString(houseId.getId())
                                        ,Q.pm("CREATE")
                                )+ ");");
                                projectWriter.flush();
                            }
                        }
                    }
                }
                projectWriter.flush();
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

            if (buildResultSet!=null){
                buildResultSet.close();
            }
            if(buildStatement!=null){
                buildStatement.close();
            }
            if(workbookResultSet!=null){
                workbookResultSet.close();
            }
            if(workbookStatement!=null){
                workbookStatement.close();
            }
            if(houseResultSet!=null){
                houseResultSet.close();
            }
            if(houseStatement!=null){
                houseStatement.close();
            }
            MyConnection.closeConnection();
        }

    }
}
