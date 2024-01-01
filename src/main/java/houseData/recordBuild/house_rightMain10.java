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

public class house_rightMain10 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static String CONTRACT_DB_URL = "jdbc:mysql://127.0.0.1:3306/CONTRACT?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String house_FILE="/house_right.sql";
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
        String ba_house_id = null;
        boolean have_contract = false;

        try{
            houseResultSet = houseStatement.executeQuery("SELECT HH.ID AS HID,HH.BUILDID,HB.PROJECT_ID,HB.MAP_CORP,HP.DEVELOPERID,HD.NAME,HC.LICENSE_NUMBER,HC.COMPANY_CER_CODE," +
                    "HP.NAME AS DNAME,HS.DISTRICT,HH.DESIGN_USE_TYPE,HH.IN_FLOOR_NAME FROM " +
                    "HOUSE_INFO.HOUSE AS HH LEFT JOIN HOUSE_INFO.BUILD AS HB ON HH.BUILDID=HB.ID " +
                    "LEFT JOIN HOUSE_INFO.PROJECT AS HP ON HB.PROJECT_ID=HP.ID LEFT JOIN HOUSE_INFO.SECTION AS HS ON HP.SECTIONID=HS.ID " + //WHERE HH.ID in ('B318N1-2-01','210681104029358','210603103001252','B544N1-4-02','0020-25','0030-0','0182-21','0181-36')
                    "LEFT JOIN HOUSE_INFO.DEVELOPER AS HD ON HP.DEVELOPERID=HD.ID " +
                    "LEFT JOIN HOUSE_INFO.ATTACH_CORPORATION AS HC ON HD.ATTACH_ID=HC.ID " +             //where HB.PROJECT_ID='206' WHERE HH.ID IN('8827','B87N2-6-02','138345') WHERE HH.ID IN('66072','66071','66073')
                    "ORDER BY HB.PROJECT_ID,HH.BUILDID,HH.ID");
            houseResultSet.last();
            int sumCount = houseResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            houseResultSet.beforeFirst();
            while (houseResultSet.next()){

                houseId = houseIdMapper.selectByOldHouseId(houseResultSet.getString("HID"));
                if(houseId==null){
                    System.out.println("houseBusinessMain5没有找到对应HOUSE_idE记录检查:--:"+houseResultSet.getString("HID"));
                    return;
                }
                ba_biz_id = null;
                ba_house_id=null;

                houseBusinessResultSet = houseBusinessStatement.executeQuery("SELECT O.ID as OID,O.DEFINE_ID,BH.ID AS BHID,H.ID AS houseBId,H.HOUSE_CODE,O.APPLY_TIME" +
                        " FROM OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
                        "LEFT JOIN HOUSE H ON BH.AFTER_HOUSE=H.ID " +
                        "WHERE O.STATUS IN ('COMPLETE') AND DEFINE_ID IN ('WP42','BL42') " +
                        "AND BH.BUSINESS_ID IS NOT NULL AND BH.HOUSE_CODE ='"+houseResultSet.getString("HID")+"'"+
                        "ORDER BY H.HOUSE_CODE,O.APPLY_TIME;");
                if(houseBusinessResultSet.next()) {
                    System.out.println("HOUSE_CODE---"+houseBusinessResultSet.getString("HOUSE_CODE"));
                    houseBusinessResultSet.beforeFirst();
                    while (houseBusinessResultSet.next()){
                        ba_biz_id = houseBusinessResultSet.getString("OID");
                        ba_house_id = houseBusinessResultSet.getString("houseBId");
                    }
                }

                if(ba_biz_id!=null && ba_house_id!=null){
                    ownerRecordHouseId = ownerRecordHouseIdMapper.selectByOldId(ba_house_id);
                    if(ownerRecordHouseId==null){
                        System.out.println("houseBusinessMain5没有找到对应记录检查ownerRecordHouseId:"+ba_house_id);
                        return;
                    }
                    houseContractResultSet = houseContractStatement.executeQuery("select * from record_building.house_rights where house_id = '"+houseId.getId()+"'");
                    houseContractResultSet.last();
                    if(houseContractResultSet.getRow()<=0){
                        contractResultSet=contractStatement.executeQuery("SELECT * FROM record_building.house as h left join record_building.house_snapshot as hs on h.house_info_id=hs.house_info_id where hs.house_id = '"+houseId.getId()+"' and hs.work_id='"+ownerRecordHouseId.getId()+"'");
                        if(contractResultSet.next()){
                            powerOwnerResultSet = powerOwnerStatement.executeQuery("SELECT PO.* from HOUSE_OWNER AS HO LEFT JOIN POWER_OWNER PO ON HO.POOL =PO.ID " +
                                    "WHERE PO.TYPE='CONTRACT' AND HO.HOUSE = '"+ba_house_id+"'");
                            if(powerOwnerResultSet.next()){
                                powerOwnerResultSet.beforeFirst();
                                while (powerOwnerResultSet.next()){
                                    powerOwnerId = powerOwnerIdMapper.selectByOldId(powerOwnerResultSet.getString("ID"));
                                    if(powerOwnerId == null){
                                        System.out.println("houseBusinessMain5-house_rights没有找到对应记录检查powerOwnerId:--:"+powerOwnerResultSet.getString("ID"));
                                        return;
                                    }
                                    have_contract = true;
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
                }


                houseBusinessWriter.flush();
                i++;
                System.out.println("sumCount---"+i+"/"+String.valueOf(sumCount));
            }

        }catch (Exception e){
            System.out.println("id is errer-----id:"+houseResultSet.getString("HID"));
            e.printStackTrace();
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
