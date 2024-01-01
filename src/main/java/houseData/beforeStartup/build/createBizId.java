package houseData.beforeStartup.build;

import com.bean.OwnerRecordHouseId;
import com.bean.OwnerRecordProjectId;
import com.bean.build.BizId;
import com.mapper.OwnerRecordHouseIdMapper;
import com.mapper.OwnerRecordProjectIdMapper;
import com.mapper.build.BizIdMapper;
import com.utils.BuildbatisUtils;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class createBizId {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static Statement projectBusinessStatement;
    private static ResultSet projectBusinessResultSet;

    private static Statement houseBusinessStatement;
    private static ResultSet houseBusinessResultSet;






    public static void main(String agr[]) throws SQLException {
        projectBusinessStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        houseBusinessStatement= MyConnection.getStatement(DB_URL,"root","dgsoft");

        SqlSession bsqlSession = BuildbatisUtils.getSqlSession();
        BizIdMapper bizIdMapper = bsqlSession.getMapper(BizIdMapper.class);


        SqlSession sqlSession = MybatisUtils.getSqlSession();
        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper = sqlSession.getMapper(OwnerRecordHouseIdMapper.class);
        OwnerRecordProjectIdMapper ownerRecordProjectIdMapper = sqlSession.getMapper(OwnerRecordProjectIdMapper.class);
        OwnerRecordProjectId ownerRecordProjectId = null;
        OwnerRecordHouseId ownerRecordHouseId=null;

        try {
            Map<String,Object> map = new HashMap<>();
//            projectBusinessResultSet=projectBusinessStatement.executeQuery("SELECT P.ID AS PID,O.ID AS OID FROM HOUSE_OWNER_RECORD.PROJECT AS P " +
//                    "LEFT JOIN HOUSE_OWNER_RECORD.PROJECT_SELL_INFO AS PI ON P.ID = PI.ID LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID " +
//                    "WHERE DEFINE_ID='WP50' " +
//                    "ORDER BY P.NAME,O.ID,O.APPLY_TIME");
//
//            projectBusinessResultSet.last();
//            int sumCount = projectBusinessResultSet.getRow(),i=0;
//            projectBusinessResultSet.beforeFirst();
//            while(projectBusinessResultSet.next()){
//                map.clear();
//                ownerRecordProjectId = ownerRecordProjectIdMapper.selectByOldId(projectBusinessResultSet.getString("PID"));
//                if(ownerRecordProjectId == null){
//                    System.out.println("没有找到对应记录检查ownerRecordProjectId--:"+projectBusinessResultSet.getString("PID"));
//                    return;
//                }
//                if(bizIdMapper.selectByOldId(projectBusinessResultSet.getString("OID")) == null) {
//                    map.put("work_id",ownerRecordProjectId.getId());
//                    map.put("business_id",projectBusinessResultSet.getString("OID"));
//                    bizIdMapper.addBizId(map);
//                    bsqlSession.commit();
//                }
//                i++;
//                System.out.println(i+"/"+String.valueOf(sumCount));
//            }



            houseBusinessResultSet = houseBusinessStatement.executeQuery("SELECT O.ID AS OID,H.ID AS HID FROM OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
                    "LEFT JOIN HOUSE AS H ON BH.AFTER_HOUSE=H.ID " +
                    "WHERE O.STATUS IN ('COMPLETE','COMPLETE_CANCEL','MODIFYING') AND DEFINE_ID IN ('WP42','BL42','WP40','WP43') " +
                    "AND BH.BUSINESS_ID IS NOT NULL " +
                    "ORDER BY H.HOUSE_CODE,O.CREATE_TIME");
            houseBusinessResultSet.last();
           int sumCount2 = houseBusinessResultSet.getRow(),j=0;
            houseBusinessResultSet.beforeFirst();
            while(houseBusinessResultSet.next()){
                ownerRecordHouseId = ownerRecordHouseIdMapper.selectByOldId(houseBusinessResultSet.getString("HID"));
                if(ownerRecordHouseId == null){
                    System.out.println("没有找到对应记录检查ownerRecordProjectId--:"+houseBusinessResultSet.getString("HID"));
                    return;
                }
                if(bizIdMapper.selectByOldId(houseBusinessResultSet.getString("OID")) == null) {
                    map.put("work_id",ownerRecordHouseId.getId());
                    map.put("business_id",houseBusinessResultSet.getString("OID"));
                    bizIdMapper.addBizId(map);
                    bsqlSession.commit();
                }


                j++;
                System.out.println(j+"/"+String.valueOf(sumCount2));

            }



        }catch (Exception e){

        }finally {

        }


    }
}
