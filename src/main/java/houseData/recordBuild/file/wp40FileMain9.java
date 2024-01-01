package houseData.recordBuild.file;

import com.bean.BizFileId;
import com.bean.HouseId;
import com.bean.OwnerRecordHouseId;
import com.bean.OwnerRecordProjectId;
import com.mapper.BizFileIdMapper;
import com.mapper.HouseIdMapper;
import com.mapper.OwnerRecordHouseIdMapper;
import com.mapper.OwnerRecordProjectIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;
import com.utils.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class wp40FileMain9 {

    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/wp40File8.sql";
    private static File businessFileFile;
    private static BufferedWriter businessFileWriter;
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

    private static Statement houseBusinessStatement;
    private static ResultSet houseBusinessResultSet;

    private static Statement maxHouseOrderStatement;
    private static ResultSet maxHouseOrderResultSet;
    public static void main(String agr[]) throws SQLException {

        businessFileFile = new File(PROJECT_FILE);
        if(businessFileFile.exists()){
            businessFileFile.delete();
        }

        try{
            businessFileFile.createNewFile();
            FileWriter fw = new FileWriter(businessFileFile.getAbsoluteFile());
            businessFileWriter = new BufferedWriter(fw);
            businessFileWriter.write("USE work;");
            businessFileWriter.flush();
        }catch (IOException e){
            System.out.println("businessFileWriter 文件创建失败");
            e.printStackTrace();
            return;
        }

        projectStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        projectBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        buildBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        landEndTimeStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        houseBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        maxHouseOrderStatement= MyConnection.getStatement(DB_URL,USER,PASSWORD);

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        OwnerRecordProjectIdMapper ownerRecordProjectIdMapper = sqlSession.getMapper(OwnerRecordProjectIdMapper.class);
        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper = sqlSession.getMapper(OwnerRecordHouseIdMapper.class);
        HouseIdMapper houseIdMapper = sqlSession.getMapper(HouseIdMapper.class);
        BizFileIdMapper bizFileIdMapper = sqlSession.getMapper(BizFileIdMapper.class);
        OwnerRecordProjectId ownerRecordProjectId = null;
        OwnerRecordHouseId ownerRecordHouseId=null;
        BizFileId bizFileId = null;
        HouseId houseId = null;
        String biz_ID = null;
        try {

            houseBusinessResultSet = houseBusinessStatement.executeQuery("SELECT * FROM OWNER_BUSINESS WHERE DEFINE_ID='WP40' AND STATUS IN ('COMPLETE') ");
            houseBusinessResultSet.last();
            int sumCount2 = houseBusinessResultSet.getRow(),j=0;

            System.out.println("记录总数-"+sumCount2);
            houseBusinessResultSet.beforeFirst();
            long ysid=2600000;
            while (houseBusinessResultSet.next()){//初始登记一个业务多个房屋，把要件绑定到同业务中房号最大的，没有汉字，特殊字符的房子中
                ownerRecordHouseId=null;
                biz_ID = null;
                maxHouseOrderResultSet = maxHouseOrderStatement.executeQuery("SELECT H.HOUSE_ORDER,H.ID,H.HOUSE_CODE,O.ID AS OID " +
                        "FROM OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
                        "LEFT JOIN HOUSE H ON BH.AFTER_HOUSE=H.ID " +
                        "WHERE O.STATUS = ('COMPLETE') AND DEFINE_ID ='WP40' " +
                        "AND BH.BUSINESS_ID IS NOT NULL AND O.ID='"+houseBusinessResultSet.getString("ID")+"' " +
                        "AND H.HOUSE_ORDER REGEXP '^[a-zA-Z0-9]+$' " +
                        "ORDER BY CAST(H.HOUSE_ORDER  AS SIGNED) DESC LIMIT 1");

                if(maxHouseOrderResultSet.next()){
                    ownerRecordHouseId = ownerRecordHouseIdMapper.selectByOldId(maxHouseOrderResultSet.getString("ID"));
                    biz_ID = maxHouseOrderResultSet.getString("OID");
                    if(ownerRecordHouseId == null){
                        System.out.println("wp40FileMain9-maxHouseOrderResultSet有找到对应记录检查ownerRecordHouseId--:"+maxHouseOrderResultSet.getString("ID"));
                        return;
                    }
                }else{//有初始登记业务，房号都是有汉字特殊字符的,取房号最长的
                    projectResultSet = projectStatement.executeQuery("SELECT H.HOUSE_ORDER,H.ID,H.HOUSE_CODE,O.ID AS OID " +
                            "FROM OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
                            "LEFT JOIN HOUSE H ON BH.AFTER_HOUSE=H.ID " +
                            "WHERE O.STATUS = ('COMPLETE') AND DEFINE_ID ='WP40' " +
                            "AND BH.BUSINESS_ID IS NOT NULL AND O.ID='"+houseBusinessResultSet.getString("ID")+"' " +
                            "ORDER BY H.HOUSE_ORDER DESC LIMIT 1; ");
                    if(projectResultSet.next()) {
                        ownerRecordHouseId = ownerRecordHouseIdMapper.selectByOldId(projectResultSet.getString("ID"));
                        biz_ID = projectResultSet.getString("OID");
                        if (ownerRecordHouseId == null) {
                            System.out.println("wp40FileMain9-projectResultSet有找到对应记录检查ownerRecordHouseId--:" + projectResultSet.getString("ID"));
                            return;
                        }
                    }

                }
                if(ownerRecordHouseId!=null && biz_ID!=null){


                    landEndTimeResultSet = landEndTimeStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUSINESS_EMP WHERE (TYPE ='APPLY_EMP') and BUSINESS_ID='"+biz_ID+"'");
                    String task_id_sl=null;
                    if(landEndTimeResultSet.next()) {
                        if(landEndTimeResultSet.getString("TYPE").equals("APPLY_EMP")){
                            task_id_sl = landEndTimeResultSet.getString("ID");
                        }else {
                            task_id_sl = null;
                        }
                    }
                    taskOperBusinessResultSet=taskOperBusinessStatement.executeQuery("select * from BUSINESS_FILE WHERE BUSINESS_ID ='"+biz_ID+"'");
                    if (taskOperBusinessResultSet.next()){
                        taskOperBusinessResultSet.beforeFirst();
                        while (taskOperBusinessResultSet.next()){
                            bizFileId = bizFileIdMapper.selectByOldId(taskOperBusinessResultSet.getString("ID"));
                            if (bizFileId==null){
                                System.out.println("没有找到对应记录检查bizFileId:"+taskOperBusinessResultSet.getString("ID"));
                                return;
                            }
                            //work.attachment
                            businessFileWriter.newLine();
                            businessFileWriter.write("INSERT work.attachment (id, name, must, have, work_id, version) VALUE ");
                            businessFileWriter.write("(" + Q.v(Long.toString(ysid),Q.pm(taskOperBusinessResultSet.getString("NAME"))
                                    ,"true","true"
                                    ,Long.toString(ownerRecordHouseId.getId()),"0"
                            )+ ");");

                            workbookResultSet = workbookStatement.executeQuery("SELECT FL.FID,FL.SHA256,FL.SIZE,FL.MIME,FL.E_TAG,UF.PRI,UF.FILE_NAME FROM UPLOAD_FILE AS UF,  " +
                                    "FILE_LINK AS FL  WHERE UF.ID=FL.OLD AND BUSINESS_FILE_ID='"+taskOperBusinessResultSet.getString("ID")+"'");
                            if(workbookResultSet.next()){
                                workbookResultSet.beforeFirst();
                                while (workbookResultSet.next()){
                                    String order_num="1";
                                    if(workbookResultSet.getString("PRI")!=null && !workbookResultSet.getString("PRI").equals("")){
                                        order_num = workbookResultSet.getString("PRI");
                                    }

                                    businessFileWriter.newLine();
                                    businessFileWriter.write("INSERT work.work_file(fid, sha256, attach_id, size, mime, e_tag, order_num, filename, task_id) value ");
                                    businessFileWriter.write("(" + Q.v(Q.pm(workbookResultSet.getString("FID")),Q.p(workbookResultSet.getString("SHA256"))
                                            ,Long.toString(ysid),Q.p(workbookResultSet.getString("SIZE"))
                                            ,Q.pm(workbookResultSet.getString("MIME")),Q.p(workbookResultSet.getString("E_TAG"))
                                            ,Q.pm(order_num),Q.p(workbookResultSet.getString("FILE_NAME"))
                                            ,Q.p(task_id_sl)
                                    )+ ");");
                                }
                            }

                            ysid++;

                        }
                    }

                }








                businessFileWriter.flush();
                j++;
                System.out.println(j+"/"+String.valueOf(sumCount2));

            }

        }catch (Exception e){
            e.printStackTrace();
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
