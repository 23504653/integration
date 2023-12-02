package houseData.recordBuild;


import com.bean.*;
import com.hp.hpl.sparta.xpath.TrueExpr;
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

//    private static Statement houseContractStatement;
//    private static ResultSet houseContractResultSet;

    private static Statement powerOwnerStatement;
    private static ResultSet powerOwnerResultSet;
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
            houseBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.building.house.register.import','房屋初始登记导入',false,true,0,'data');");
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
//        HouseContractIdMapper houseContractIdMapper = sqlSession.getMapper(HouseContractIdMapper.class);
        HouseUseTypeMapper houseUseTypeMapper = sqlSession.getMapper(HouseUseTypeMapper.class);
        OtherHouseTypeMapper otherHouseTypeMapper = sqlSession.getMapper(OtherHouseTypeMapper.class);
        FloorBeginEndMapper floorBeginEndMapper = sqlSession.getMapper(FloorBeginEndMapper.class);
        PowerOwnerIdMapper powerOwnerIdMapper = sqlSession.getMapper(PowerOwnerIdMapper.class);

        houseBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectCardStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
//        houseContractStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        powerOwnerStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);

        PowerOwnerId powerOwnerId = null;
        LockedHouseId lockedHouseId= null;
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        FloorBeginEnd floorBeginEnd = new FloorBeginEnd();
        BuildId buildId =null;
        HouseId houseId = null;
        ProjectId projectId = null;
        OwnerRecordHouseId ownerRecordHouseId=null;
        OwnerRecordHouseId afterOwnerRecordHouseId =null;
        OwnerRecordProjectId ownerRecordProjectId = null;
        OwnerRecordBuildId ownerRecordBuildId=null;
//        HouseContractId houseContractId = null;
        HouseUseType houseUseType= new HouseUseType();
        OtherHouseType otherHouseType = null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null,DEFINE_ID=null;
        String nowId=null,beforeId=null,EMP_NAME=null,oldProjectId=null,before_info_id_build = null;
        OwnerRecordProjectId selectBizBusiness =new OwnerRecordProjectId();
        OwnerRecordBuildId beforeBuildId = null;
        try{
            houseResultSet = houseStatement.executeQuery("SELECT HH.ID AS HID,HH.BUILDID,HB.PROJECT_ID,HB.MAP_CORP,HP.DEVELOPERID,HD.NAME,HC.LICENSE_NUMBER," +
                    "HP.NAME AS DNAME,HS.DISTRICT,HH.DESIGN_USE_TYPE,HH.IN_FLOOR_NAME FROM " +
                    "HOUSE_INFO.HOUSE AS HH LEFT JOIN HOUSE_INFO.BUILD AS HB ON HH.BUILDID=HB.ID " +
                    "LEFT JOIN HOUSE_INFO.PROJECT AS HP ON HB.PROJECT_ID=HP.ID LEFT JOIN HOUSE_INFO.SECTION AS HS ON HP.SECTIONID=HS.ID " +
                    "LEFT JOIN HOUSE_INFO.DEVELOPER AS HD ON HP.DEVELOPERID=HD.ID " +
                    "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS HC ON HD.ATTACH_ID=HC.ID " +
                    "WHERE HH.ID IN ('B544N1-4-02','0020-25','0030-0');"); //,'0020-25','0030-0'
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
                    developName = houseResultSet.getString("NAME");
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
                houseBusinessResultSet = houseBusinessStatement.executeQuery("SELECT O.ID as OID,O.DEFINE_ID,BH.*,BH.ID AS BHID,H.ID AS houseBId,H.PROJECT_CODE,O.*,H.*" +
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

                            //HOUSE.PROJECT_CODE 查询预售许可证的业务的项目楼幢信息
                            projectCardResultSet = projectCardStatement.executeQuery("select P.ID,P.PROJECT_CODE,O.ID AS OID,B.ID AS BID,O.SELECT_BUSINESS FROM OWNER_BUSINESS AS O " +
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

                                //work_business.before_info_id 获取
                                before_info_id = Long.toString(projectId.getId());
                                oldProjectId = null;
                                if(projectCardResultSet.getString("SELECT_BUSINESS")!=null
                                        &&  !projectCardResultSet.getString("SELECT_BUSINESS").isBlank()){
                                    workbookResultSet = workbookStatement.executeQuery("SELECT O.*,P.ID AS PID FROM HOUSE_OWNER_RECORD.PROJECT AS P LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID" +
                                            " WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL','MODIFYING','RUNNING') AND DEFINE_ID='WP50'" +
                                            " AND O.ID='"+projectCardResultSet.getString("SELECT_BUSINESS")+"'");
                                    if(workbookResultSet.next()){
                                        //System.out.println(projectBusinessResultSet.getString("SELECT_BUSINESS")+"--"+workbookResultSet.getString("PID"));
                                        selectBizBusiness = ownerRecordProjectIdMapper.selectByOldId(workbookResultSet.getString("PID"));

                                        if(selectBizBusiness==null){
                                            before_info_id = null;
                                            System.out.println("houseBusinessMain5没有找到对应记录检查ownerRecordProjectIdMapper --- selectBizBusiness:"+workbookResultSet.getString("PID"));
                                            return;
                                        }else {
                                            before_info_id = Long.toString(selectBizBusiness.getId());
                                            oldProjectId = workbookResultSet.getString("PID");
                                        }
                                    }
                                }

                               //build_business.before_info_id_build
                                before_info_id_build = Long.toString(buildId.getId());//默认
                                workbookResultSet = workbookStatement.executeQuery("SELECT HB.* FROM HOUSE_OWNER_RECORD.PROJECT AS P LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID " +
                                        "LEFT JOIN BUILD AS HB ON P.ID= HB.PROJECT " +
                                        "WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL','MODIFYING') AND DEFINE_ID='WP50' " +
                                        " AND O.ID='"+projectCardResultSet.getString("SELECT_BUSINESS")+"'"+
                                        "AND P.ID='"+oldProjectId+"' AND BUILD_CODE='"+houseBusinessResultSet.getString("BUILD_CODE")+"'");
                                if(workbookResultSet.next()) {
                                    if (workbookResultSet.getString("ID") != null &&
                                            !workbookResultSet.getString("ID").equals("")) {
                                        beforeBuildId = ownerRecordBuildIdMapper.selectByOldBuildId(workbookResultSet.getString("ID"));
                                        if (beforeBuildId == null) {
                                            System.out.println("houseBusinessMain5-没有找到对应beforeBuildId记录检查:--BUILD_CODE:" + houseBusinessResultSet.getString("BUILD_CODE") + "---oldProjectId:" + oldProjectId);
                                            return;
                                        } else {
                                            before_info_id_build = Long.toString(beforeBuildId.getId());
                                        }
                                    }
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
//                            nowId = houseBusinessResultSet.getString("houseBId"); 测试上下手用
                            nowId = Long.toString(ownerRecordHouseId.getId());
                            if(beforeId == null){
                                beforeId = Long.toString(houseId.getId());
                            }

                            houseUseType = houseUseTypeMapper.selectByDesignUseType(houseResultSet.getString("DESIGN_USE_TYPE"));
                            if(houseUseType ==null){//房屋类型不是其它
                                System.out.println("houseBusinessMain5没有找到对应记录检查houseUseType:"+houseResultSet.getString("ID"));
                                return;
                            }
                            if(houseUseType.getHouseType().equals("Null")){//DesignUseType 用途是其它 需要调 otherHouseType表 进行房屋类型的取值
                                otherHouseType = otherHouseTypeMapper.selectByHouseId(houseResultSet.getString("ID"));
                                if (otherHouseType == null){
                                    System.out.println("houseBusinessMain5没有找到对应记录检查otherHouseTypeMapper:"+houseResultSet.getString("ID"));
                                    return;
                                }else {
                                    houseUseType.setHouseType(otherHouseType.getHouseType());
                                }
                            }

                            floorBeginEnd = floorBeginEndMapper.selectByName(houseResultSet.getString("IN_FLOOR_NAME"));
                            if(floorBeginEnd==null){
                                System.out.println("houseBusinessMain5没有找到对应记录检查floorBeginEndMapper:"+houseResultSet.getString("ID"));
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

//                                houseContractResultSet = houseContractStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.HOUSE_CONTRACT WHERE ID = '"+houseBusinessResultSet.getString("BHID")+"'");
//                                houseContractResultSet.next();
//                                houseContractId = houseContractIdMapper.selectByOldId(houseContractResultSet.getString("ID"));
//                                if(houseContractId==null){
//                                    System.out.println("houseBusinessMain5没有找到对应记录检查houseContractId:--:"+houseContractResultSet.getString("ID"));
//                                    return;
//                                }
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
                                //apartment_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT apartment_snapshot (apartment_info_id,layer_type, layer_type_key," +
                                        " house_type, floor_begin, floor_end, floor_name," +
                                        "unit, use_type, use_type_key, apartment_number) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.p(FindWorkBook.structure(houseBusinessResultSet.getString("STRUCTURE")).getValue())
                                        ,Q.p(FindWorkBook.structure(houseBusinessResultSet.getString("STRUCTURE")).getId()),Q.pm(houseUseType.getHouseType())
                                        ,Integer.toString(floorBeginEnd.getBeginFloor())
                                        ,Integer.toString(floorBeginEnd.getEndFloor()),Q.pm(houseBusinessResultSet.getString("IN_FLOOR_NAME"))
                                        ,Q.pm(houseBusinessResultSet.getString("HOUSE_UNIT_NAME")),Q.pm(houseUseType.getLabel())
                                        ,Integer.toString(houseUseType.getValue()),Q.pm(houseBusinessResultSet.getString("HOUSE_ORDER"))
                                )+ ");");

                                //apartment_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT apartment_mapping_snapshot (mapping_info_id, area, area_use, area_share, area_loft,mapping_corp_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(houseBusinessResultSet.getBigDecimal("HOUSE_AREA"))
                                        ,Q.pm(houseBusinessResultSet.getBigDecimal("USE_AREA")),Q.pm(houseBusinessResultSet.getBigDecimal("COMM_AREA"))
                                        ,Q.pm(houseBusinessResultSet.getBigDecimal("LOFT_AREA")),Q.p(FindWorkBook.getMappingCorpId(houseResultSet.getString("MAP_CORP")).getId())
                                )+ ");");

                                //house_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_snapshot (HOUSE_INFO_ID, HOUSE_ID, MAPPING_INFO_ID, APARTMENT_INFO_ID, WORK_ID, UNIT_CODE) value  ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,Long.toString(ownerRecordHouseId.getId()),Q.p(houseBusinessResultSet.getString("UNIT_NUMBER"))
                                )+ ");");


                                //HOUSE
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("update house set updated_at = '" + houseBusinessResultSet.getTimestamp("CREATE_TIME") +"',house_info_id='"+ownerRecordHouseId.getId()+"' WHERE house_id='" + houseId.getId() + "';");

                                //project_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                                        "developer_name, work_type,business_id,before_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(projectId.getId())
                                        ,Q.pm(UNIFIED_ID),Long.toString(ownerRecordProjectId.getId())
                                        ,Q.pm(developName),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),before_info_id
                                )+ ");");

                                //build_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(projectId.getId()),Q.pm(Q.nowFormatTime())
                                        ,Long.toString(ownerRecordBuildId.getId()),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),Q.p(before_info_id_build)
                                )+ ");");

                                //house_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(buildId.getId()),Q.pm(Q.nowFormatTime())
                                        ,Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,Q.pm("BUSINESS"),Q.pm(beforeId)
                                )+ ");");

                                //new_house_contract_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT new_house_contract_business (contract_id, work_id, license_id, valid, version, registering_house, house_id) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,Q.pm(UNIFIED_ID),FindWorkBook.getContractStatus(houseBusinessResultSet.getString("STATUS"))
                                        ,"1","false"
                                        ,Long.toString(houseId.getId())
                                ) + ");");

                                //contract_business_transferee
                                powerOwnerResultSet = powerOwnerStatement.executeQuery("SELECT PO.* from HOUSE_OWNER AS HO LEFT JOIN POWER_OWNER PO ON HO.POOL =PO.ID " +
                                        "WHERE HO.HOUSE = '"+houseBusinessResultSet.getString("houseBId")+"'");
                                if(powerOwnerResultSet.next()){
                                    powerOwnerResultSet.beforeFirst();
                                    while (powerOwnerResultSet.next()){
                                        powerOwnerId = powerOwnerIdMapper.selectByOldId(powerOwnerResultSet.getString("ID"));
                                        if(powerOwnerId == null){
                                           System.out.println("houseBusinessMain5没有找到对应记录检查powerOwnerId:--:"+powerOwnerResultSet.getString("ID"));
                                           return;
                                        }

                                        houseBusinessWriter.newLine();
                                        houseBusinessWriter.write("INSERT contract_business_transferee (ID, ID_TYPE, ID_NUMBER, NAME, WORK_ID, TEL) VALUE ");
                                        houseBusinessWriter.write("(" + Q.v(Long.toString(powerOwnerId.getId()),Q.pm(FindWorkBook.changeIdType(powerOwnerResultSet.getString("ID_TYPE")).getId())
                                                ,Q.pm(powerOwnerResultSet.getString("ID_NO")),Q.pm(powerOwnerResultSet.getString("NAME"))
                                                ,Long.toString(ownerRecordHouseId.getId()),Q.pm(powerOwnerResultSet.getString("PHONE"))
                                        ) + ");");
                                    }
                                }

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
                                //apartment_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT apartment_snapshot (apartment_info_id,layer_type, layer_type_key," +
                                        " house_type, floor_begin, floor_end, floor_name," +
                                        "unit, use_type, use_type_key, apartment_number) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.p(FindWorkBook.structure(houseBusinessResultSet.getString("STRUCTURE")).getValue())
                                        ,Q.p(FindWorkBook.structure(houseBusinessResultSet.getString("STRUCTURE")).getId()),Q.pm(houseUseType.getHouseType())
                                        ,Integer.toString(floorBeginEnd.getBeginFloor())
                                        ,Integer.toString(floorBeginEnd.getEndFloor()),Q.pm(houseBusinessResultSet.getString("IN_FLOOR_NAME"))
                                        ,Q.pm(houseBusinessResultSet.getString("HOUSE_UNIT_NAME")),Q.pm(houseUseType.getLabel())
                                        ,Integer.toString(houseUseType.getValue()),Q.pm(houseBusinessResultSet.getString("HOUSE_ORDER"))
                                )+ ");");

                                //apartment_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT apartment_mapping_snapshot (mapping_info_id, area, area_use, area_share, area_loft,mapping_corp_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(houseBusinessResultSet.getBigDecimal("HOUSE_AREA"))
                                        ,Q.pm(houseBusinessResultSet.getBigDecimal("USE_AREA")),Q.pm(houseBusinessResultSet.getBigDecimal("COMM_AREA"))
                                        ,Q.pm(houseBusinessResultSet.getBigDecimal("LOFT_AREA")),Q.p(FindWorkBook.getMappingCorpId(houseResultSet.getString("MAP_CORP")).getId())
                                )+ ");");

                                //house_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_snapshot (HOUSE_INFO_ID, HOUSE_ID, MAPPING_INFO_ID, APARTMENT_INFO_ID, WORK_ID, UNIT_CODE) value  ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,Long.toString(ownerRecordHouseId.getId()),Q.p(houseBusinessResultSet.getString("UNIT_NUMBER"))
                                )+ ");");


                                //HOUSE
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("update house set updated_at = '" + houseBusinessResultSet.getTimestamp("CREATE_TIME") +"',house_info_id='"+ownerRecordHouseId.getId()+"' WHERE house_id='" + houseId.getId() + "';");

                                //project_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                                        "developer_name, work_type,business_id,before_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(projectId.getId())
                                        ,Q.pm(UNIFIED_ID),Long.toString(ownerRecordProjectId.getId())
                                        ,Q.pm(developName),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),before_info_id
                                )+ ");");

                                //build_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(projectId.getId()),Q.pm(Q.nowFormatTime())
                                        ,Long.toString(ownerRecordBuildId.getId()),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),Q.p(before_info_id_build)
                                )+ ");");


                                workbookResultSet=workbookStatement.executeQuery("select BH.AFTER_HOUSE from OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID\n" +
                                        "WHERE O.ID = '"+houseBusinessResultSet.getString("SELECT_BUSINESS")+"'");
                                if(workbookResultSet.next()){
                                    afterOwnerRecordHouseId = ownerRecordHouseIdMapper.selectByOldId(workbookResultSet.getString("AFTER_HOUSE"));
                                    if(afterOwnerRecordHouseId==null){
                                        System.out.println("houseBusinessMain5没有找到对应记录检查afterOwnerRecordHouseId:--:"+workbookResultSet.getString("SELECT_BUSINESS"));
                                        return;
                                    }
                                }
//                                System.out.println("afterOwnerRecordHouseId.getId()---"+afterOwnerRecordHouseId.getId());
                                //house_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(buildId.getId()),Q.pm(Q.nowFormatTime())
                                        ,Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,Q.pm("BUSINESS"),Q.pm(Long.toString(afterOwnerRecordHouseId.getId()))
                                )+ ");");

                                //contract_cancel_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT contract_cancel_business (WORK_ID, CONTRACT_ID, VALID, VERSION, HOUSE_ID, TARGET_WORK_ID) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,FindWorkBook.getContractStatus(houseBusinessResultSet.getString("STATUS")),"1"
                                        ,Long.toString(houseId.getId()),Long.toString(afterOwnerRecordHouseId.getId())
                                ) + ");");
                            }


                            if(DEFINE_ID.equals("WP40")){
                                //work ownerRecordHouseId.getId() 作为workId
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()), Q.pm("OLD")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , Q.pm("房屋初始登记导入"), Q.pm("COMPLETED")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , "0", Q.pm("func.building.house.register.import")
                                        , "true", Q.pm("business")
                                ) + ");");
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

                                //apartment_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT apartment_snapshot (apartment_info_id,layer_type, layer_type_key," +
                                        " house_type, floor_begin, floor_end, floor_name," +
                                        "unit, use_type, use_type_key, apartment_number) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.p(FindWorkBook.structure(houseBusinessResultSet.getString("STRUCTURE")).getValue())
                                        ,Q.p(FindWorkBook.structure(houseBusinessResultSet.getString("STRUCTURE")).getId()),Q.pm(houseUseType.getHouseType())
                                        ,Integer.toString(floorBeginEnd.getBeginFloor())
                                        ,Integer.toString(floorBeginEnd.getEndFloor()),Q.pm(houseBusinessResultSet.getString("IN_FLOOR_NAME"))
                                        ,Q.pm(houseBusinessResultSet.getString("HOUSE_UNIT_NAME")),Q.pm(houseUseType.getLabel())
                                        ,Integer.toString(houseUseType.getValue()),Q.pm(houseBusinessResultSet.getString("HOUSE_ORDER"))
                                )+ ");");

                                //apartment_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT apartment_mapping_snapshot (mapping_info_id, area, area_use, area_share, area_loft,mapping_corp_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(houseBusinessResultSet.getBigDecimal("HOUSE_AREA"))
                                        ,Q.pm(houseBusinessResultSet.getBigDecimal("USE_AREA")),Q.pm(houseBusinessResultSet.getBigDecimal("COMM_AREA"))
                                        ,Q.pm(houseBusinessResultSet.getBigDecimal("LOFT_AREA")),Q.p(FindWorkBook.getMappingCorpId(houseResultSet.getString("MAP_CORP")).getId())
                                )+ ");");

                                //house_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_snapshot (HOUSE_INFO_ID, HOUSE_ID, MAPPING_INFO_ID, APARTMENT_INFO_ID, WORK_ID, UNIT_CODE,register_info_id) value  ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,Long.toString(ownerRecordHouseId.getId()),Q.p(houseBusinessResultSet.getString("UNIT_NUMBER"))
                                        ,Long.toString(ownerRecordHouseId.getId())
                                )+ ");");


                                //HOUSE
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("update house set updated_at = '" + houseBusinessResultSet.getTimestamp("CREATE_TIME") +"',house_info_id='"+ownerRecordHouseId.getId()+"' WHERE house_id='" + houseId.getId() + "';");

                                //project_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                                        "developer_name, work_type,business_id,before_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(projectId.getId())
                                        ,Q.pm(UNIFIED_ID),Long.toString(ownerRecordProjectId.getId())
                                        ,Q.pm(developName),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),before_info_id
                                )+ ");");

                                //build_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(projectId.getId()),Q.pm(Q.nowFormatTime())
                                        ,Long.toString(ownerRecordBuildId.getId()),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),Q.p(before_info_id_build)
                                )+ ");");

                                //house_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(buildId.getId()),Q.pm(Q.nowFormatTime())
                                        ,Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,Q.pm("BUSINESS"),Q.pm(beforeId)
                                )+ ");");

                                //house_register_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_register_business (work_id, house_id, work_type, business_id) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Q.pm("CREATE"),Long.toString(ownerRecordHouseId.getId())
                                )+ ");");

                                workbookResultSet = workbookStatement.executeQuery("select * from HOUSE_OWNER_RECORD.MAKE_CARD WHERE BUSINESS_ID='"+houseBusinessResultSet.getString("OID")+"'");
                                if(workbookResultSet.next()){
                                    //house_register_snapshot
                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT house_register_snapshot (register_info_id, register_number, register_date, register_gov, register_type)  value ");
                                    houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(workbookResultSet.getString("NUMBER"))
                                            ,Q.pm(houseBusinessResultSet.getTimestamp("APPLY_TIME")),Q.pm("东港市房地产管理处")
                                            ,Q.pm("HOUSE")
                                    )+ ");");
                                }











                            }













                            System.out.println("BIZID--"+houseBusinessResultSet.getString("OID")+"-"+houseBusinessResultSet.getString("DEFINE_NAME")+"---houseCode--:"+houseBusinessResultSet.getString("HOUSE_CODE")+"----nowID--:"+nowId+"---beforeId--"+beforeId);
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
