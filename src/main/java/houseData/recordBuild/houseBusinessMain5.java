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
    private static String CONTRACT_DB_URL = "jdbc:mysql://127.0.0.1:3306/CONTRACT?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String house_FILE="/hoseBusinessRecord5.sql";
    private static File houseBusinessFile;
    private static BufferedWriter houseBusinessWriter;

    private static Statement houseBusinessStatement;
    private static ResultSet houseBusinessResultSet;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

    private static Statement projectCardStatement;
    private static ResultSet projectCardResultSet;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;

    private static Statement houseContractStatement;
    private static ResultSet houseContractResultSet;

    private static Statement powerOwnerStatement;
    private static ResultSet powerOwnerResultSet;

    private static Statement contractStatement;
    private static ResultSet contractResultSet;

    private static Statement businessPoolStatement;
    private static ResultSet businessPoolResultSet;

    private static Statement contractOtherStatement;
    private static ResultSet contractOtherResultSet;

    private static Set<String> DEAL_DEFINE_ID= new HashSet<>();


    public static void main(String agr[]) throws SQLException {


        houseBusinessFile = new File(house_FILE);
        if (houseBusinessFile.exists()) {
            houseBusinessFile.delete();
        }

        try{
            houseBusinessFile.createNewFile();
            FileWriter fw = new FileWriter(houseBusinessFile.getAbsoluteFile());
            houseBusinessWriter = new BufferedWriter(fw);
            houseBusinessWriter.write("USE record_building;");
//            houseBusinessWriter.newLine();
//            houseBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('process_sale_contract_new','商品房合同备案导入',false,true,0,'business');");
//            houseBusinessWriter.newLine();
//            houseBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('process_sale_contract_new_cancel','商品房合同备案撤消导入',false,true,0,'business');");
//            houseBusinessWriter.newLine();
//            houseBusinessWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.building.house.register','房屋初始登记导入',false,true,0,'data');");
            houseBusinessWriter.flush();

        }catch (IOException e){
            System.out.println("houseBusinessFile 文件创建失败");
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
        ContractBusinessPoolIdMapper contractBusinessPoolIdMapper = sqlSession.getMapper(ContractBusinessPoolIdMapper.class);
        ContractPowerProxyIdMapper contractPowerProxyIdMapper = sqlSession.getMapper(ContractPowerProxyIdMapper.class);

        houseBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectCardStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseContractStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        powerOwnerStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        //Contract
        contractStatement = MyConnection.getStatement(CONTRACT_DB_URL,USER,PASSWORD);
        businessPoolStatement = MyConnection.getStatement(CONTRACT_DB_URL,USER,PASSWORD);
        contractOtherStatement = MyConnection.getStatement(CONTRACT_DB_URL,USER,PASSWORD);

        // 用于保存前一行的 HOUSE_CODE
        String previousHouseCode = null;
        PowerOwnerId powerOwnerId = null;
        LockedHouseId lockedHouseId= null;
        JointCorpDevelop jointCorpDevelop = new JointCorpDevelop();
        FloorBeginEnd floorBeginEnd = new FloorBeginEnd();
        BuildId buildId =null;
        HouseId houseId = null;
        ProjectId projectId = null;
        OwnerRecordHouseId ownerRecordHouseId=null;
        OwnerRecordHouseId afterOwnerRecordHouseId =null;
        OwnerRecordProjectId ownerRecordProjectId = new OwnerRecordProjectId();
        OwnerRecordBuildId ownerRecordBuildId= new OwnerRecordBuildId();
//        HouseContractId houseContractId = null;
        HouseUseType houseUseType= new HouseUseType();
        OtherHouseType otherHouseType = null;
        String developName=null,UNIFIED_ID=null,districtCode=null,before_info_id=null,DEFINE_ID=null;
        String nowId=null,beforeId=null,EMP_NAME=null,oldProjectId=null,before_info_id_build = null;
        OwnerRecordProjectId selectBizBusiness =new OwnerRecordProjectId();
        OwnerRecordBuildId beforeBuildId = null;
        ContractBusinessPoolId contractBusinessPoolId = null;
        ContractPowerProxyId contractPowerProxyId = null;
        boolean house_registered = false,build_completed=false;
        String developer_info_id = null;
        String license_id;
        String ba_biz_id = null;
        try{
            houseResultSet = houseStatement.executeQuery("SELECT HH.ID AS HID,HH.BUILDID,HB.PROJECT_ID,HB.MAP_CORP,HP.DEVELOPERID,HD.NAME,HC.LICENSE_NUMBER,HC.COMPANY_CER_CODE," +
                    "HP.NAME AS DNAME,HS.DISTRICT,HH.DESIGN_USE_TYPE,HH.IN_FLOOR_NAME FROM " +
                    "HOUSE_INFO.HOUSE AS HH LEFT JOIN HOUSE_INFO.BUILD AS HB ON HH.BUILDID=HB.ID " +
                    "LEFT JOIN HOUSE_INFO.PROJECT AS HP ON HB.PROJECT_ID=HP.ID LEFT JOIN HOUSE_INFO.SECTION AS HS ON HP.SECTIONID=HS.ID " + //WHERE HH.ID in ('210681104029358','210603103001252','B544N1-4-02','0020-25','0030-0','0182-21')
                    "LEFT JOIN HOUSE_INFO.DEVELOPER AS HD ON HP.DEVELOPERID=HD.ID " +
                    "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS HC ON HD.ATTACH_ID=HC.ID " +             //where HB.PROJECT_ID='206' WHERE HH.ID IN('8827','B87N2-6-02','138345') WHERE HH.ID IN('66072','66071','66073')
                    "ORDER BY HB.PROJECT_ID,HH.BUILDID,HH.ID"); //'210603103001252','B544N1-4-02','0020-25','0030-0','0182-21',133939 WHERE HB.PROJECT_ID IN ('206') WHERE HH.ID='21068110141843255051200488' 21.34 WHERE HB.PROJECT_ID='115'
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

                //开发商备案信息developer_info
                developer_info_id = null;
                if(houseResultSet.getString("COMPANY_CER_CODE")!=null &&
                        !houseResultSet.getString("COMPANY_CER_CODE").equals("")){
                    developer_info_id = Long.toString(jointCorpDevelop.getCorpId());
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
                        String unit = null;
                        if(houseBusinessResultSet.getString("HOUSE_UNIT_NAME")!=null && !houseBusinessResultSet.getString("HOUSE_UNIT_NAME").equals("")){
                            unit = houseBusinessResultSet.getString("HOUSE_UNIT_NAME").replace("单元", "");
                        }
                        DEFINE_ID = houseBusinessResultSet.getString("DEFINE_ID");
                        if(DEFINE_ID.equals("WP42") || DEFINE_ID.equals("BL42") || DEFINE_ID.equals("WP43")|| DEFINE_ID.equals("WP40")){
                            build_completed = false;
                            //HOUSE.PROJECT_CODE 查询预售许可证的业务的项目楼幢信息
                            projectCardResultSet = projectCardStatement.executeQuery("select P.ID,P.PROJECT_CODE,O.ID AS OID,B.ID AS BID,O.SELECT_BUSINESS,PSI.TYPE " +
                                    "FROM OWNER_BUSINESS AS O " +
                                    ",HOUSE_OWNER_RECORD.PROJECT AS P,HOUSE_OWNER_RECORD.PROJECT_SELL_INFO AS PSI,HOUSE_OWNER_RECORD.BUILD B " +
                                    "WHERE O.ID=P.BUSINESS AND P.ID=PSI.ID AND P.ID=B.PROJECT " +
                                    "AND  O.DEFINE_ID IN ('WP50') AND STATUS IN ('COMPLETE','COMPLETE_CANCEL','MODIFYING') and B.BUILD_CODE ='"+houseResultSet.getString("BUILDID")+"'");
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
                                if(projectCardResultSet.getString("TYPE")!=null &&
                                        !projectCardResultSet.getString("TYPE").equals("")
                                        && projectCardResultSet.getString("TYPE").equals("NOW_SELL")){

                                    build_completed = true;
                                }else{
                                    build_completed = false;
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
                                System.out.println("houseBusinessMain5没有找到对应记录检查houseUseType:"+houseResultSet.getString("HID"));
                                return;
                            }
                            if(houseUseType.getHouseType().equals("Null")){//DesignUseType 用途是其它 需要调 otherHouseType表 进行房屋类型的取值
                                otherHouseType = otherHouseTypeMapper.selectByHouseId(houseResultSet.getString("HID"));
                                if (otherHouseType == null){
                                    System.out.println("houseBusinessMain5没有找到对应记录检查otherHouseTypeMapper:"+houseResultSet.getString("HID"));
                                    return;
                                }else {
                                    houseUseType.setHouseType(otherHouseType.getHouseType());
                                }
                            }

                            floorBeginEnd = floorBeginEndMapper.selectByName(houseResultSet.getString("IN_FLOOR_NAME"));
                            if(floorBeginEnd==null){
                                System.out.println("houseBusinessMain5没有找到对应记录检查floorBeginEndMapper:"+houseResultSet.getString("HID"));
                                return;
                            }
                            String HOUSE_STATUS = null;
                            workbookResultSet = workbookStatement.executeQuery("select HOUSE_STATUS from HOUSE_OWNER_RECORD.HOUSE_RECORD where HOUSE_CODE='"+houseResultSet.getString("HID")+"'");
                            if(workbookResultSet.next()){
                                if (workbookResultSet.getString("HOUSE_STATUS")!=null &&
                                        !workbookResultSet.getString("HOUSE_STATUS").equals("")){
                                    HOUSE_STATUS = workbookResultSet.getString("HOUSE_STATUS");
                                }

                            }
                           //是否有预售许可证号
                            workbookResultSet = workbookStatement.executeQuery("SELECT HB.BUILD_CODE FROM HOUSE_OWNER_RECORD.PROJECT_CARD AS PC LEFT JOIN HOUSE_OWNER_RECORD.MAKE_CARD AS MA ON PC.ID = MA.ID " +
                                    "LEFT JOIN  HOUSE_OWNER_RECORD.PROJECT AS P ON PC.PROJECT= P.ID " +
                                    "LEFT JOIN HOUSE_OWNER_RECORD.BUILD AS HB ON P.ID = HB.PROJECT " +
                                    "LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID " +
                                    "WHERE MA.TYPE='PROJECT_RSHIP' AND O.DEFINE_ID IN ('WP50') AND O.STATUS IN ('COMPLETE','COMPLETE_CANCEL','MODIFYING') " +
                                    "and HB.BUILD_CODE='"+houseResultSet.getString("BUILDID")+"'");
                            workbookResultSet.last();
                            license_id = Long.toString(buildId.getId());
                            if(workbookResultSet.getRow()==0){
                                license_id = Long.toString(buildId.getId());
                            }else {
                                license_id = Long.toString(ownerRecordProjectId.getId());
                            }

//                            System.out.println("license_id--:"+license_id);
                            if( DEFINE_ID.equals("WP42") || (DEFINE_ID.equals("BL42") && HOUSE_STATUS!=null && !HOUSE_STATUS.equals("INIT_REG"))) {
                                //work ownerRecordHouseId.getId() 作为workId
                                System.out.println("STATUS--"+houseBusinessResultSet.getString("STATUS"));
                                if(houseBusinessResultSet.getString("STATUS").equals("COMPLETE")){
                                    ba_biz_id = houseBusinessResultSet.getString("OID");
                                }

                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()), Q.pm("OLD")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , Q.pm("商品房合同备案导入"), Q.pm("COMPLETED")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , "0", Q.pm("process_sale_contract_new")
                                        , "true", Q.pm("business")
                                ) + ");");

                                //操作人员记录
                                //开发商合同提交人
                                taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.TASK_OPER WHERE OPER_TYPE='CREATE' AND TASK_NAME='提交合同' AND BUSINESS='" + houseBusinessResultSet.getString("OID") + "'");
                                if (taskOperBusinessResultSet.next()){
                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                                    houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()), Q.pm("TASK")
                                            , Q.pm(taskOperBusinessResultSet.getString("EMP_CODE")), Q.pm(taskOperBusinessResultSet.getString("EMP_NAME"))
                                            , Q.pm(taskOperBusinessResultSet.getString("ID")), Q.pm(taskOperBusinessResultSet.getTimestamp("OPER_TIME"))
                                    ) + ");");

                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT work_task (task_id, message, task_name, pass) VALUE ");
                                    houseBusinessWriter.write("(" + Q.v(Q.pm(taskOperBusinessResultSet.getString("ID")), Q.pm("网签合同创建")
                                            , Q.pm("提交合同人"), Q.p(true)
                                    ) + ");");

                                }
                                taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUSINESS_EMP WHERE BUSINESS_ID='" + houseBusinessResultSet.getString("OID") + "'");
                                if (taskOperBusinessResultSet.next()) {
                                    taskOperBusinessResultSet.beforeFirst();
                                    while (taskOperBusinessResultSet.next()) {

                                        houseBusinessWriter.newLine();
                                        houseBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                                        houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()), Q.pm("TASK")
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
                                        ,Q.pm(unit),Q.pm(houseUseType.getLabel())
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
                                houseBusinessWriter.write("update house set updated_at = '" + houseBusinessResultSet.getTimestamp("CREATE_TIME")
                                        +"',status='RECORD"
                                        +"',house_info_id='"+ownerRecordHouseId.getId()+"' WHERE house_id='" + houseId.getId() + "';");

                                //project_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                                        "developer_name, work_type,business_id,before_info_id,updated_at,developer_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(projectId.getId())
                                        ,Q.pm(UNIFIED_ID),Long.toString(ownerRecordProjectId.getId())
                                        ,Q.pm(developName),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),before_info_id
                                        ,Q.pm(houseBusinessResultSet.getTimestamp("CREATE_TIME"))
                                        ,developer_info_id
                                )+ ");");

                                //build_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(projectId.getId()),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        ,Long.toString(ownerRecordBuildId.getId()),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),Q.p(before_info_id_build)
                                )+ ");");

                                //house_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(buildId.getId()),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        ,Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,Q.pm("BUSINESS"),Q.pm(beforeId)
                                )+ ");");

                                //new_house_contract_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT new_house_contract_business (contract_id, work_id, license_id, valid, version, registering_house, house_id,file_uploaded) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,license_id,FindWorkBook.getContractStatus(houseBusinessResultSet.getString("STATUS"))
                                        ,"1","false"
                                        ,Long.toString(houseId.getId()),"false"
                                ) + ");");

                                //contract_business_transferee
                                powerOwnerResultSet = powerOwnerStatement.executeQuery("SELECT PO.* from HOUSE_OWNER AS HO LEFT JOIN POWER_OWNER PO ON HO.POOL =PO.ID " +
                                        "WHERE PO.TYPE='CONTRACT' AND HO.HOUSE = '"+houseBusinessResultSet.getString("houseBId")+"'");
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

                                //System.out.println("wp42-houseCode----"+houseResultSet.getString("HID"));
                                //只有一条补录或者备案业务时，写入
                                taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("select DEFINE_ID from OWNER_BUSINESS as O left join BUSINESS_HOUSE as BH " +
                                        "on O.ID = BH.BUSINESS_ID WHERE O.STATUS='COMPLETE' and HOUSE_CODE='"+houseResultSet.getString("HID")+"' GROUP BY O.DEFINE_ID");
                                if(taskOperBusinessResultSet.next()){
                                    taskOperBusinessResultSet.last();
                                    int count=taskOperBusinessResultSet.getRow();
                                    if(count == 1 && (taskOperBusinessResultSet.getString("DEFINE_ID").equals("WP42")
                                            || taskOperBusinessResultSet.getString("DEFINE_ID").equals("BL42"))){
                                       // System.out.println("wp42-houseCode----"+houseResultSet.getString("HID")+taskOperBusinessResultSet.getString("DEFINE_ID"));
                                       //house_rights
                                        powerOwnerResultSet = powerOwnerStatement.executeQuery("SELECT PO.* from HOUSE_OWNER AS HO LEFT JOIN POWER_OWNER PO ON HO.POOL =PO.ID " +
                                                "WHERE PO.TYPE='CONTRACT' AND HO.HOUSE = '"+houseBusinessResultSet.getString("houseBId")+"'");
                                        if(powerOwnerResultSet.next()){
                                            powerOwnerResultSet.beforeFirst();
                                            while (powerOwnerResultSet.next()){
                                                powerOwnerId = powerOwnerIdMapper.selectByOldId(powerOwnerResultSet.getString("ID"));
                                                if(powerOwnerId == null){
                                                    System.out.println("houseBusinessMain5-house_rights没有找到对应记录检查powerOwnerId:--:"+powerOwnerResultSet.getString("ID"));
                                                    return;
                                                }
                                                houseBusinessWriter.newLine();
                                                houseBusinessWriter.write("INSERT house_rights (id, house_id, power_type, work_id, id_type, id_number, name, tel) VALUE ");
                                                houseBusinessWriter.write("(" + Q.v(Long.toString(powerOwnerId.getId()),Long.toString(houseId.getId())
                                                        ,Q.pm("CONTRACT"),Long.toString(ownerRecordHouseId.getId())
                                                        ,Q.pm(FindWorkBook.changeIdType(powerOwnerResultSet.getString("ID_TYPE")).getId()),Q.pm(powerOwnerResultSet.getString("ID_NO"))
                                                        ,Q.pm(powerOwnerResultSet.getString("NAME")),Q.pm(powerOwnerResultSet.getString("PHONE"))
                                                ) + ");");
                                            }
                                        }

                                    }

                                }



                                //导入开发商客户端合同状态STATUS='SUBMIT'，在内网中业务中的有俩种处理方式一种是直接导入成已备案，一种是不倒 BHID=BUSINESS_HOUSE.ID
                                //备案外网提交和内网生成的都导入record_contract

                                String Contract_Type = "CANCELLED";
                                if(houseBusinessResultSet.getString("STATUS").equals("COMPLETE")){
                                    Contract_Type = "EFFECTIVE";
                                }
                                if (houseBusinessResultSet.getString("SOURCE").equals("BIZ_OUTSIDE") && DEFINE_ID.equals("WP42")){
                                    //houseContractResultSet = house_owner_record.house_Contract
                                   houseContractResultSet = houseContractStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.HOUSE_CONTRACT WHERE ID = '"+houseBusinessResultSet.getString("BHID")+"'");
                                   if(houseContractResultSet.next()){
                                       contractResultSet = contractStatement.executeQuery("SELECT CHC.ID AS HCID,CHC.*,ROUND(CHC.CONTRACT_PRICE/CHC.HOUSE_AREA,3) as unit_price,CPP.*,NHC.PROJECT_CER_NUMBER" +
                                               " FROM CONTRACT.HOUSE_CONTRACT AS CHC LEFT JOIN CONTRACT.SALE_PROXY_PERSON AS CPP ON CHC.ID=CPP.ID " +
                                               "LEFT JOIN CONTRACT.NEW_HOUSE_CONTRACT AS NHC ON CHC.ID=NHC.ID " +
                                               "WHERE CHC.STATUS ='SUBMIT' AND CHC.ID ='"+houseContractResultSet.getString("CONTRACT_NUMBER")+"' AND CHC.HOUSE_CODE='"+houseResultSet.getString("HID")+"'");
                                       if(contractResultSet.next()){
                                           //submit_info
                                           houseBusinessWriter.newLine();
                                           houseBusinessWriter.write("INSERT record_contract.submit_info (contract_id, work_id, user, fid, created_at, unit_price) VALUE ");
                                           houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                                   ,Q.pm(contractResultSet.getString("ATTACH_EMP_ID")),Q.pm("123")
                                                   ,Q.pm(contractResultSet.getTimestamp("CREATE_TIME")),Q.pm(contractResultSet.getBigDecimal("unit_price"))
                                           ) + ");");
                                           //house_contract
                                           houseBusinessWriter.newLine();
                                           houseBusinessWriter.write("INSERT record_contract.house_contract (contract_id, status, type, created_at, updated_at, version, unified_id, record_at) value ");
                                           houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(Contract_Type)
                                                   ,Q.pm(FindWorkBook.getContractType(contractResultSet.getString("TYPE"))),Q.pm(contractResultSet.getTimestamp("CREATE_TIME"))
                                                   ,Q.pm(contractResultSet.getTimestamp("CREATE_TIME")),Q.pm("0")
                                                   ,Q.pm(UNIFIED_ID),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                           ) + ");");

                                           //corp_proxy_person
                                           houseBusinessWriter.newLine();
                                           houseBusinessWriter.write("INSERT record_contract.corp_proxy_person (contract_id, id_type, id_number, name, tel, version) value ");
                                           houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(FindWorkBook.changeIdType(contractResultSet.getString("ID_TYPE")).getId())
                                                   ,Q.pm(contractResultSet.getString("ID_NO")),Q.pm(contractResultSet.getString("NAME"))
                                                   ,Q.pm(contractResultSet.getString("PHONE")),Q.pm("0")
                                           ) + ");");

                                           //contract_content
                                           houseBusinessWriter.newLine();
                                           houseBusinessWriter.write("INSERT record_contract.contract_content (contract_id, pricing_method, total_price, deposit, pay_type, initial_pay, version) value ");
                                           houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm("OTHER")
                                                   ,Q.pm(contractResultSet.getBigDecimal("CONTRACT_PRICE")),"0"
                                                   ,Q.pm(FindWorkBook.getPayType(contractResultSet.getString("SALE_PAY_TYPE"))),"0"
                                                   ,"0"
                                           ) + ");");

                                           //contract_rights
                                           String share_relation=null,proxy_id=null;
                                           businessPoolResultSet = businessPoolStatement.executeQuery("SELECT * FROM CONTRACT.BUSINESS_POOL WHERE CONTRACT='"+contractResultSet.getString("ID")+"'");
                                           if(businessPoolResultSet.next()){
                                               share_relation = businessPoolResultSet.getString("RELATION");
                                               businessPoolResultSet.beforeFirst();
                                               while (businessPoolResultSet.next()){
                                                   contractBusinessPoolId = contractBusinessPoolIdMapper.selectByOldId(businessPoolResultSet.getString("ID"));
                                                   if(contractBusinessPoolId == null){
                                                       System.out.println("houseBusinessMain5没有找到对应记录检查contractBusinessPoolId:--:"+businessPoolResultSet.getString("ID"));
                                                       return;
                                                   }
                                                   //contract_rights 查询有沒有代理人 有的話先插入contract_rights，Type='Transferee'
                                                   contractOtherResultSet = contractOtherStatement.executeQuery("SELECT * FROM CONTRACT.POWER_PROXY_PERSON WHERE ID='"+businessPoolResultSet.getString("ID")+"'");
                                                   if(contractOtherResultSet.next()){
                                                       contractPowerProxyId = contractPowerProxyIdMapper.selectByOldId(contractOtherResultSet.getString("ID"));
                                                       if(contractPowerProxyId == null){
                                                           System.out.println("houseBusinessMain5没有找到对应记录检查contractPowerProxyId:--:"+contractOtherResultSet.getString("ID"));
                                                           return;
                                                       }
                                                       proxy_id = Long.toString(contractPowerProxyId.getId());
                                                       houseBusinessWriter.newLine();
                                                       houseBusinessWriter.write("INSERT record_contract.contract_rights (id, type, rights_type, name, id_type, " +
                                                               "id_number, tel, " +
                                                               "post_code,sex, birthday, address, " +
                                                               "contract_id) VALUE ");
                                                       houseBusinessWriter.write("(" + Q.v(Long.toString(contractPowerProxyId.getId()),Q.pm(FindWorkBook.getProxyType(contractOtherResultSet.getString("TYPE")))
                                                               ,Q.pm("Transferee"),Q.pm(contractOtherResultSet.getString("NAME"))
                                                               ,Q.pm(FindWorkBook.changeIdType(contractOtherResultSet.getString("ID_TYPE")).getId()),Q.pm(contractOtherResultSet.getString("ID_NO"))
                                                               ,Q.p(contractOtherResultSet.getString("PHONE")),Q.p(contractOtherResultSet.getString("POST_CODE"))
                                                               ,Q.p(contractOtherResultSet.getString("SEX")),Q.p(contractOtherResultSet.getTimestamp("BIRTHDAY"))
                                                               ,Q.p(contractOtherResultSet.getString("ADDRESS")),Long.toString(ownerRecordHouseId.getId())
                                                       ) + ");");

                                                   }

                                                   houseBusinessWriter.newLine();
                                                   houseBusinessWriter.write("INSERT record_contract.contract_rights (id, type, rights_type, name, id_type, " +
                                                           "id_number, percentage, tel, " +
                                                           "legal_name, post_code,sex, birthday, address, " +
                                                           "proxy_id, contract_id) VALUE ");
                                                   houseBusinessWriter.write("(" + Q.v(Long.toString(contractBusinessPoolId.getId()),Q.pm("Main")
                                                           ,Q.pm("Transferee"),Q.pm(businessPoolResultSet.getString("NAME"))
                                                           ,Q.pm(FindWorkBook.changeIdType(businessPoolResultSet.getString("ID_TYPE")).getId()),Q.pm(businessPoolResultSet.getString("ID_NO"))
                                                           ,Q.p(businessPoolResultSet.getBigDecimal("PERC")),Q.p(businessPoolResultSet.getString("PHONE"))
                                                           ,Q.p(businessPoolResultSet.getString("LEGAL_PERSON")),Q.p(businessPoolResultSet.getString("POST_CODE"))
                                                           ,Q.p(businessPoolResultSet.getString("SEX")),Q.p(businessPoolResultSet.getTimestamp("BIRTHDAY"))
                                                           ,Q.p(businessPoolResultSet.getString("ADDRESS")),Q.p(proxy_id)
                                                           ,Long.toString(ownerRecordHouseId.getId())
                                                   ) + ");");
                                               }
                                           }
                                           //contract_transferee
                                           houseBusinessWriter.newLine();
                                           houseBusinessWriter.write("INSERT record_contract.contract_transferee (contract_id, owner_type, share_relation, share_relation_text, version) value ");
                                           houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(FindWorkBook.getPoolType(contractResultSet.getString("POOL_TYPE")))
                                                   ,Q.p(FindWorkBook.getPoolRelation(share_relation).getId()),Q.p(FindWorkBook.getPoolRelation(share_relation).getValue())
                                                   ,"0"
                                           ) + ");");
                                           //contract_house_snapshot
                                           workbookResultSet = workbookStatement.executeQuery("select * from HOUSE_OWNER_RECORD.MAKE_CARD as MC,HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O,HOUSE_OWNER_RECORD.BUSINESS_HOUSE AS BH" +
                                                   "  WHERE MC.BUSINESS_ID=O.ID AND MC.BUSINESS_ID=BH.BUSINESS_ID AND O.DEFINE_ID='WP40' AND BH.HOUSE_CODE='"+houseBusinessResultSet.getString("HOUSE_CODE")+"'");
                                           house_registered = false;
                                           workbookResultSet.last();
                                           if(workbookResultSet.getRow()>1) {
                                               house_registered = true;
                                           }
                                           houseBusinessWriter.newLine();
                                           houseBusinessWriter.write("INSERT record_contract.contract_house_snapshot (contract_id, house_id, house_info_id, license_id, " +
                                                   "build_id, build_info_id, project_id, project_info_id, district_code, project_name, build_name, " +
                                                   "apartment_number, unit, house_address, area, area_use, build_completed, house_registered,created_at) value ");
                                           houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                                   ,Long.toString(ownerRecordHouseId.getId()),license_id
                                                   ,Long.toString(buildId.getId()),Long.toString(ownerRecordBuildId.getId())
                                                   ,Long.toString(projectId.getId()),Long.toString(ownerRecordProjectId.getId())
                                                   ,Q.pm(houseResultSet.getString("DISTRICT")),Q.pm(houseBusinessResultSet.getString("DISTRICT_NAME"))
                                                   ,Q.pm(buildId.getBuildName()),Q.pm(houseBusinessResultSet.getString("HOUSE_ORDER"))
                                                   ,Q.pm(unit),Q.pm(houseBusinessResultSet.getString("ADDRESS"))
                                                   ,Q.pm(houseBusinessResultSet.getBigDecimal("HOUSE_AREA")),Q.pm(houseBusinessResultSet.getBigDecimal("USE_AREA"))
                                                   ,Boolean.toString(build_completed),Boolean.toString(house_registered),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                           ) + ");");
                                       }
                                   }
                                }else{
                                    houseContractResultSet = houseContractStatement.executeQuery("SELECT HC.*,H.HOUSE_AREA,ROUND(HC.SUM_PRICE/H.HOUSE_AREA,3) AS unit_price FROM HOUSE_OWNER_RECORD.HOUSE_CONTRACT AS HC " +
                                            "LEFT JOIN HOUSE_OWNER_RECORD.BUSINESS_HOUSE BH ON HC.ID=BH.ID " +
                                            "LEFT JOIN HOUSE_OWNER_RECORD.HOUSE AS H ON BH.AFTER_HOUSE=H.ID WHERE HC.ID = '"+houseBusinessResultSet.getString("BHID")+"'");
                                    if(houseContractResultSet.next()){
                                        //submit_info
                                        houseBusinessWriter.newLine();
                                        houseBusinessWriter.write("INSERT record_contract.submit_info (contract_id, work_id, user, fid, created_at, unit_price) VALUE ");
                                        houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                                ,Q.pm("ROOT"),Q.pm("数据导入")
                                                ,Q.pm(houseBusinessResultSet.getTimestamp("CREATE_TIME")),Q.pm(houseContractResultSet.getBigDecimal("unit_price"))
                                        ) + ");");
                                    }

                                    //System.out.println("123123-"+houseContractResultSet.getString("TYPE"));
                                    //house_contract
                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT record_contract.house_contract (contract_id, status, type, created_at, updated_at, version, unified_id, record_at) value ");
                                    houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(Contract_Type)
                                            ,Q.pm(FindWorkBook.getContractType(houseContractResultSet.getString("TYPE"))),Q.pm(houseBusinessResultSet.getTimestamp("CREATE_TIME"))
                                            ,Q.pm(houseBusinessResultSet.getTimestamp("CREATE_TIME")),Q.pm("0")
                                            ,Q.pm(UNIFIED_ID),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                    ) + ");");

                                    //corp_proxy_person
                                    workbookResultSet = workbookStatement.executeQuery("SELECT * FROM BUSINESS_PERSION WHERE TYPE='PRE_SALE_ENTRUST' AND BUSINESS_ID ='"+houseBusinessResultSet.getString("OID")+"'");
                                    if(workbookResultSet.next()){
                                        houseBusinessWriter.newLine();
                                        houseBusinessWriter.write("INSERT record_contract.corp_proxy_person (contract_id, id_type, id_number, name, tel, version) value ");
                                        houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(FindWorkBook.changeIdType(workbookResultSet.getString("ID_TYPE")).getId())
                                                ,Q.pm(workbookResultSet.getString("ID_NO")),Q.pm(workbookResultSet.getString("NAME"))
                                                ,Q.pm(workbookResultSet.getString("PHONE")),Q.pm("0")
                                        ) + ");");
                                    }

                                    //contract_content
                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT record_contract.contract_content (contract_id, pricing_method, total_price, deposit, pay_type, initial_pay, version) value ");
                                    houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm("OTHER")
                                            ,Q.pm(houseContractResultSet.getBigDecimal("SUM_PRICE")),"0"
                                            ,Q.pm(FindWorkBook.getPayType(houseContractResultSet.getString("PAY_TYPE"))),"0"
                                            ,"0"
                                    ) + ");");


                                    String pool_memo= "SINGLE_OWNER",share_relation=null;
                                    powerOwnerResultSet = powerOwnerStatement.executeQuery("SELECT PO.* from HOUSE_OWNER AS HO LEFT JOIN POWER_OWNER PO ON HO.POOL =PO.ID " +
                                            "WHERE PO.TYPE='CONTRACT' AND HO.HOUSE = '"+houseBusinessResultSet.getString("houseBId")+"'");
                                    if(powerOwnerResultSet.next()){
                                        powerOwnerResultSet.last();
                                        int poolcount = powerOwnerResultSet.getRow();
                                        if(houseBusinessResultSet.getString("POOL_MEMO")!=null && !houseBusinessResultSet.getString("POOL_MEMO").equals("")){
                                            pool_memo= houseBusinessResultSet.getString("POOL_MEMO");
                                        }else{
                                            if(poolcount == 1){
                                                pool_memo = "SINGLE_OWNER";
                                            }else if(poolcount == 2){
                                                pool_memo = "TOGETHER_OWNER";
                                            }else if(poolcount > 2){
                                                pool_memo = "SHARE_OWNER";
                                            }
                                        }
                                        powerOwnerResultSet.beforeFirst();
                                        while (powerOwnerResultSet.next()){
                                            powerOwnerId = powerOwnerIdMapper.selectByOldId(powerOwnerResultSet.getString("ID"));
                                            if(powerOwnerId == null) {
                                                System.out.println("houseBusinessMain5-house_rights没有找到对应记录检查powerOwnerId12:--:" + powerOwnerResultSet.getString("ID"));
                                                return;
                                            }
                                            share_relation = powerOwnerResultSet.getString("RELATION");
                                            houseBusinessWriter.newLine();
                                            houseBusinessWriter.write("INSERT record_contract.contract_rights (id, type, rights_type, name, id_type, " +
                                                    "id_number, percentage, tel, " +
                                                    "legal_name,sex, birthday, address, " +
                                                    "contract_id) VALUE ");
                                            houseBusinessWriter.write("(" + Q.v(Long.toString(powerOwnerId.getId()),Q.pm("Main")
                                                    ,Q.pm("Transferee"),Q.pm(powerOwnerResultSet.getString("NAME"))
                                                    ,Q.pm(FindWorkBook.changeIdType(powerOwnerResultSet.getString("ID_TYPE")).getId()),Q.pm(powerOwnerResultSet.getString("ID_NO"))
                                                    ,Q.p(powerOwnerResultSet.getBigDecimal("PERC")),Q.p(powerOwnerResultSet.getString("PHONE"))
                                                    ,Q.p(powerOwnerResultSet.getString("LEGAL_PERSON"))
                                                    ,Q.p(powerOwnerResultSet.getString("SEX")),Q.p(powerOwnerResultSet.getTimestamp("BIRTHDAY"))
                                                    ,Q.p(powerOwnerResultSet.getString("ADDRESS"))
                                                    ,Long.toString(ownerRecordHouseId.getId())
                                            ) + ");");
                                        }
                                    }else {//HOUSE_OWNER为空没有备案人

                                        houseBusinessWriter.newLine();
                                        houseBusinessWriter.write("INSERT record_contract.contract_rights (id, type, rights_type, name, id_type, " +
                                                "id_number, "+
                                                "contract_id) VALUE ");
                                        houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm("Main")
                                                ,Q.pm("Transferee"),Q.pm("未知")
                                                ,Q.pm("RESIDENT_ID"),Q.pm("未知")
                                                ,Long.toString(ownerRecordHouseId.getId())
                                        ) + ");");

                                    }
                                    //contract_transferee
                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT record_contract.contract_transferee (contract_id, owner_type, share_relation, share_relation_text, version) value ");
                                    houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(FindWorkBook.getPoolType(pool_memo))
                                            ,Q.p(FindWorkBook.getPoolRelation(share_relation).getId()),Q.p(FindWorkBook.getPoolRelation(share_relation).getValue())
                                            ,"0"
                                    ) + ");");

                                    //contract_house_snapshot
                                    workbookResultSet = workbookStatement.executeQuery("select * from HOUSE_OWNER_RECORD.MAKE_CARD as MC,HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O,HOUSE_OWNER_RECORD.BUSINESS_HOUSE AS BH" +
                                            "  WHERE MC.BUSINESS_ID=O.ID AND MC.BUSINESS_ID=BH.BUSINESS_ID AND O.DEFINE_ID='WP40' AND BH.HOUSE_CODE='"+houseBusinessResultSet.getString("HOUSE_CODE")+"'");
                                    house_registered = false;
                                    workbookResultSet.last();
                                    if(workbookResultSet.getRow()>1) {
                                        house_registered = true;
                                    }
                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT record_contract.contract_house_snapshot (contract_id, house_id, house_info_id, license_id, " +
                                            "build_id, build_info_id, project_id, project_info_id, district_code, project_name, build_name, " +
                                            "apartment_number, unit, house_address, area, area_use, build_completed, house_registered,created_at) value ");
                                    houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                            ,Long.toString(ownerRecordHouseId.getId()),license_id
                                            ,Long.toString(buildId.getId()),Long.toString(ownerRecordBuildId.getId())
                                            ,Long.toString(projectId.getId()),Long.toString(ownerRecordProjectId.getId())
                                            ,Q.pm(houseResultSet.getString("DISTRICT")),Q.pm(houseBusinessResultSet.getString("DISTRICT_NAME"))
                                            ,Q.pm(buildId.getBuildName()),Q.pm(houseBusinessResultSet.getString("HOUSE_ORDER"))
                                            ,Q.pm(unit),Q.pm(houseBusinessResultSet.getString("ADDRESS"))
                                            ,Q.pm(houseBusinessResultSet.getBigDecimal("HOUSE_AREA")),Q.pm(houseBusinessResultSet.getBigDecimal("USE_AREA"))
                                            ,Boolean.toString(build_completed),Boolean.toString(house_registered),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                    ) + ");");

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
                                        , "0", Q.pm("process_sale_contract_new_cancel")
                                        , "true", Q.pm("business")
                                ) + ");");
                                //操作人员记录
                                taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUSINESS_EMP WHERE BUSINESS_ID='" + houseBusinessResultSet.getString("OID") + "'");
                                if (taskOperBusinessResultSet.next()) {
                                    taskOperBusinessResultSet.beforeFirst();
                                    while (taskOperBusinessResultSet.next()) {

                                        houseBusinessWriter.newLine();
                                        houseBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                                        houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()), Q.pm("TASK")
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
                                        ,Q.pm(unit),Q.pm(houseUseType.getLabel())
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
                                houseBusinessWriter.write("update house set updated_at = '" + houseBusinessResultSet.getTimestamp("CREATE_TIME")
                                        +"',status='SALE"
                                        +"',house_info_id='"+ownerRecordHouseId.getId()+"' WHERE house_id='" + houseId.getId() + "';");

                                //project_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT project_business (work_id, project_id, developer_id, info_id, " +
                                        "developer_name, work_type,business_id,before_info_id,updated_at,developer_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(projectId.getId())
                                        ,Q.pm(UNIFIED_ID),Long.toString(ownerRecordProjectId.getId())
                                        ,Q.pm(developName),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),before_info_id
                                        ,Q.pm(houseBusinessResultSet.getTimestamp("CREATE_TIME"))
                                        ,developer_info_id
                                )+ ");");

                                //build_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(projectId.getId()),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        ,Long.toString(ownerRecordBuildId.getId()),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),Q.p(before_info_id_build)
                                )+ ");");


                               // System.out.println("111110-"+houseBusinessResultSet.getString("SELECT_BUSINESS"));
                                workbookResultSet=workbookStatement.executeQuery("select BH.AFTER_HOUSE from OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
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
                                        ,Long.toString(buildId.getId()),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
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

                            if(DEFINE_ID.equals("WP40") || (DEFINE_ID.equals("BL42") && HOUSE_STATUS!=null && HOUSE_STATUS.equals("INIT_REG"))){
                                //work ownerRecordHouseId.getId() 作为workId
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()), Q.pm("OLD")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , Q.pm("房屋初始登记导入"), Q.pm("COMPLETED")
                                        , Q.pm(houseBusinessResultSet.getString("CREATE_TIME")), Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        , "0", Q.pm("func.building.house.register")
                                        , "true", Q.pm("business")
                                ) + ");");
                                //操作人员记录
                                taskOperBusinessResultSet = taskOperBusinessStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUSINESS_EMP WHERE BUSINESS_ID='" + houseBusinessResultSet.getString("OID") + "'");
                                if (taskOperBusinessResultSet.next()) {
                                    taskOperBusinessResultSet.beforeFirst();
                                    while (taskOperBusinessResultSet.next()) {

//                                        System.out.println("ownerRecordHouseId----"+ownerRecordHouseId.getId()+"--taskOperBusinessResultSet.getRow()---"+ taskOperBusinessResultSet.getRow());

                                        String task_id=Long.toString(ownerRecordHouseId.getId())+"-"+Integer.toString(taskOperBusinessResultSet.getRow());
//                                        System.out.println("task_id--"+task_id);
                                        houseBusinessWriter.newLine();
                                        houseBusinessWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,WORK_TIME) VALUE ");
                                        houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()), Q.pm("TASK")
                                                , Q.pm(taskOperBusinessResultSet.getString("EMP_CODE")), Q.pm(taskOperBusinessResultSet.getString("EMP_NAME"))
                                                , Q.pm(task_id), Q.pm(taskOperBusinessResultSet.getTimestamp("OPER_TIME"))
                                        ) + ");");

                                        houseBusinessWriter.newLine();
                                        houseBusinessWriter.write("INSERT work_task (task_id, message, task_name, pass) VALUE ");
                                        houseBusinessWriter.write("(" + Q.v(Q.pm(task_id), Q.pm("同意")
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
                                        ,Q.pm(unit),Q.pm(houseUseType.getLabel())
                                        ,Integer.toString(houseUseType.getValue()),Q.pm(houseBusinessResultSet.getString("HOUSE_ORDER"))
                                )+ ");");

                                //apartment_snapshot
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT apartment_mapping_snapshot (mapping_info_id, area, area_use, area_share, area_loft,mapping_corp_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm(houseBusinessResultSet.getBigDecimal("HOUSE_AREA"))
                                        ,Q.pm(houseBusinessResultSet.getBigDecimal("USE_AREA")),Q.pm(houseBusinessResultSet.getBigDecimal("COMM_AREA"))
                                        ,Q.pm(houseBusinessResultSet.getBigDecimal("LOFT_AREA")),Q.p(FindWorkBook.getMappingCorpId(houseResultSet.getString("MAP_CORP")).getId())
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
                                }else{
                                    houseBusinessWriter.newLine();
                                    houseBusinessWriter.write("INSERT house_register_snapshot (register_info_id, register_number, register_date, register_gov, register_type)  value ");
                                    houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Q.pm("未知")
                                            ,Q.pm(houseBusinessResultSet.getTimestamp("APPLY_TIME")),Q.pm("村镇办")
                                            ,Q.pm("HOUSE")
                                    )+ ");");
                                }

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
                                        "developer_name, work_type,business_id,before_info_id,updated_at,developer_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(projectId.getId())
                                        ,Q.pm(UNIFIED_ID),Long.toString(ownerRecordProjectId.getId())
                                        ,Q.pm(developName),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),before_info_id
                                        ,Q.pm(houseBusinessResultSet.getTimestamp("CREATE_TIME"))
                                        ,developer_info_id
                                )+ ");");

                                //build_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT build_business (work_id, build_id,project_id, updated_at, info_id, work_type, business_id,before_info_id) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(buildId.getId())
                                        ,Long.toString(projectId.getId()),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        ,Long.toString(ownerRecordBuildId.getId()),Q.pm("REFER")
                                        ,Long.toString(ownerRecordHouseId.getId()),Q.p(before_info_id_build)
                                )+ ");");

                                //house_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_business (work_id, house_id, build_id, updated_at, info_id, business_id, work_type,before_info_id) VALUE ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Long.toString(buildId.getId()),Q.pm(houseBusinessResultSet.getString("CREATE_TIME"))
                                        ,Long.toString(ownerRecordHouseId.getId()),Long.toString(ownerRecordHouseId.getId())
                                        ,Q.pm("BUSINESS"),Q.pm(beforeId)
                                )+ ");");

                                //house_register_business
                                houseBusinessWriter.newLine();
                                houseBusinessWriter.write("INSERT house_register_business (work_id, house_id, work_type, business_id) value ");
                                houseBusinessWriter.write("(" + Q.v(Long.toString(ownerRecordHouseId.getId()),Long.toString(houseId.getId())
                                        ,Q.pm("CREATE"),Long.toString(ownerRecordHouseId.getId())
                                )+ ");");




                            }
//210681104029358
                            System.out.println("BIZID--"+houseBusinessResultSet.getString("OID")+"-"+houseBusinessResultSet.getString("DEFINE_NAME")+"---houseCode--:"+houseBusinessResultSet.getString("HOUSE_CODE")+"----nowID--:"+nowId+"---beforeId--"+beforeId);
                            String currentHouseCode = houseResultSet.getString("HID");
                            //System.out.println("previousHouseCode---"+previousHouseCode+"--currentHouseCode--"+currentHouseCode);
                            if(previousHouseCode != null && previousHouseCode.equals(currentHouseCode)){

                                System.out.println("ba_biz_id--"+ba_biz_id);
                               if(ba_biz_id!=null) {//最后一手生效的合同备案人
//                                   System.out.println("HOUSE_CODE changed from " + previousHouseCode + " to " + currentHouseCode +
//                                           ", BUSINESS_ID of the last record: " + nowId);
                                    //house_rights
                                    powerOwnerResultSet = powerOwnerStatement.executeQuery("SELECT PO.* from HOUSE_OWNER AS HO LEFT JOIN POWER_OWNER PO ON HO.POOL =PO.ID " +
                                            "WHERE PO.TYPE='CONTRACT' AND HO.HOUSE = '"+houseBusinessResultSet.getString("houseBId")+"'");
                                    if(powerOwnerResultSet.next()){
                                        powerOwnerResultSet.beforeFirst();
                                        while (powerOwnerResultSet.next()){
                                            powerOwnerId = powerOwnerIdMapper.selectByOldId(powerOwnerResultSet.getString("ID"));
                                            if(powerOwnerId == null){
                                                System.out.println("houseBusinessMain5-house_rights没有找到对应记录检查powerOwnerId:--:"+powerOwnerResultSet.getString("ID"));
                                                return;
                                            }

                                            houseBusinessWriter.newLine();
                                            houseBusinessWriter.write("INSERT house_rights (id, house_id, power_type, work_id, id_type, id_number, name, tel) VALUE ");
                                            houseBusinessWriter.write("(" + Q.v(Long.toString(powerOwnerId.getId()),Long.toString(houseId.getId())
                                                    ,Q.pm("CONTRACT"),Long.toString(ownerRecordHouseId.getId())
                                                    ,Q.pm(FindWorkBook.changeIdType(powerOwnerResultSet.getString("ID_TYPE")).getId()),Q.pm(powerOwnerResultSet.getString("ID_NO"))
                                                    ,Q.pm(powerOwnerResultSet.getString("NAME")),Q.pm(powerOwnerResultSet.getString("PHONE"))
                                            ) + ");");
                                        }
                                    }

                                }

                            }
                            // 更新前一行的值
                            previousHouseCode = currentHouseCode;
                            beforeId = nowId;
                            ba_biz_id = null;
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

            if(houseResultSet!=null){
                houseResultSet.close();
            }

            if (houseStatement!=null){
                houseStatement.close();
            }

            if(houseBusinessResultSet!=null){
                houseBusinessResultSet.close();
            }

            if (houseBusinessStatement!=null){
                houseBusinessStatement.close();
            }

            if(projectCardResultSet!=null){
                projectCardResultSet.close();
            }

            if (projectCardStatement!=null){
                projectCardStatement.close();
            }

            if(taskOperBusinessResultSet!=null){
                taskOperBusinessResultSet.close();
            }

            if (taskOperBusinessStatement!=null){
                taskOperBusinessStatement.close();
            }
            if(workbookResultSet!=null){
                workbookResultSet.close();
            }

            if (workbookStatement!=null){
                workbookStatement.close();
            }

            if(powerOwnerResultSet!=null){
                powerOwnerResultSet.close();
            }

            if (powerOwnerStatement!=null){
                powerOwnerStatement.close();
            }

            if(houseContractResultSet!=null){
                houseContractResultSet.close();
            }

            if (houseContractStatement!=null){
                houseContractStatement.close();
            }

            if(businessPoolResultSet!=null){
                businessPoolResultSet.close();
            }

            if (businessPoolStatement!=null){
                businessPoolStatement.close();
            }

            if(contractOtherResultSet!=null){
                contractOtherResultSet.close();
            }

            if (contractOtherStatement!=null){
                contractOtherStatement.close();
            }

            MyConnection.closeConnection();

        }





    }
}
