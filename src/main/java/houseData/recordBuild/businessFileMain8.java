package houseData.recordBuild;

import com.bean.BizFileId;
import com.bean.OwnerRecordHouseId;
import com.bean.OwnerRecordProjectId;
import com.mapper.BizFileIdMapper;
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

public class businessFileMain8 {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/businessFile8.sql";
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

        SqlSession sqlSession = MybatisUtils.getSqlSession();
        OwnerRecordProjectIdMapper ownerRecordProjectIdMapper = sqlSession.getMapper(OwnerRecordProjectIdMapper.class);
        OwnerRecordHouseIdMapper ownerRecordHouseIdMapper = sqlSession.getMapper(OwnerRecordHouseIdMapper.class);
        BizFileIdMapper bizFileIdMapper = sqlSession.getMapper(BizFileIdMapper.class);
        OwnerRecordProjectId ownerRecordProjectId = null;
        OwnerRecordHouseId ownerRecordHouseId=null;
        BizFileId bizFileId = null;

        try {

            projectBusinessResultSet = projectBusinessStatement.executeQuery("SELECT O.ID AS OID,P.ID AS PID FROM HOUSE_OWNER_RECORD.PROJECT AS P "
                    +"LEFT JOIN HOUSE_OWNER_RECORD.PROJECT_SELL_INFO AS PI ON P.ID = PI.ID LEFT JOIN HOUSE_OWNER_RECORD.OWNER_BUSINESS AS O ON P.BUSINESS = O.ID "
                    +"WHERE O.STATUS IN('COMPLETE','COMPLETE_CANCEL') AND DEFINE_ID='WP50' "
                    +"ORDER BY P.NAME,O.ID,O.APPLY_TIME;");

            projectBusinessResultSet.last();
            int sumCount = projectBusinessResultSet.getRow(),i=0;

            System.out.println("记录总数-"+sumCount);
            projectBusinessResultSet.beforeFirst();

            while (projectBusinessResultSet.next()) {

                ownerRecordProjectId = ownerRecordProjectIdMapper.selectByOldId(projectBusinessResultSet.getString("PID"));
                if(ownerRecordProjectId == null){
                    System.out.println("businessFileMain8有找到对应记录检查ownerRecordProjectId--:"+projectBusinessResultSet.getString("PID"));
                    return;
                }

                landEndTimeResultSet = landEndTimeStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUSINESS_EMP WHERE (TYPE ='APPLY_EMP') and BUSINESS_ID='"+projectBusinessResultSet.getString("OID")+"'");
                String task_id_sl=null;
                if(landEndTimeResultSet.next()) {
                    if(landEndTimeResultSet.getString("TYPE").equals("APPLY_EMP")){
                        task_id_sl = landEndTimeResultSet.getString("ID");

                    }else {
                        task_id_sl = null;
                    }
                }
                taskOperBusinessResultSet=taskOperBusinessStatement.executeQuery("select * from BUSINESS_FILE WHERE BUSINESS_ID ='"+projectBusinessResultSet.getString("OID")+"'");
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
                        businessFileWriter.write("(" + Q.v(Long.toString(bizFileId.getId()),Q.pm(taskOperBusinessResultSet.getString("NAME"))
                                ,"true","true"
                                ,Long.toString(ownerRecordProjectId.getId()),"0"
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
                                        ,Long.toString(bizFileId.getId()),Q.p(workbookResultSet.getString("SIZE"))
                                        ,Q.pm(workbookResultSet.getString("MIME")),Q.p(workbookResultSet.getString("E_TAG"))
                                        ,Q.pm(order_num),Q.p(workbookResultSet.getString("FILE_NAME"))
                                        ,Q.p(task_id_sl)
                                )+ ");");
                            }
                        }
                    }
                }
                businessFileWriter.flush();
                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }
            System.out.println("预售许可证完成");
            houseBusinessResultSet = houseBusinessStatement.executeQuery("SELECT O.ID as OID,BH.ID AS BHID,H.ID AS houseBId " +
                    " FROM OWNER_BUSINESS AS O LEFT JOIN BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
                    "LEFT JOIN HOUSE H ON BH.AFTER_HOUSE=H.ID " +
                    "WHERE O.STATUS IN ('COMPLETE','COMPLETE_CANCEL','MODIFYING') AND DEFINE_ID IN ('WP42','BL42','WP40','WP43') " +
                    "AND BH.BUSINESS_ID IS NOT NULL "+
                    "ORDER BY H.HOUSE_CODE,O.CREATE_TIME;");

            houseBusinessResultSet.last();
            int sumCount2 = houseBusinessResultSet.getRow(),j=0;
            houseBusinessResultSet.beforeFirst();
            System.out.println("记录总数-"+sumCount2);
            if (houseBusinessResultSet.next()) {
                houseBusinessResultSet.beforeFirst();
                while (houseBusinessResultSet.next()) {


                    ownerRecordHouseId = ownerRecordHouseIdMapper.selectByOldId(houseBusinessResultSet.getString("houseBId"));
                    if(ownerRecordHouseId==null){
                        System.out.println("businessFileMain8没有找到对应记录检查ownerRecordHouseId:"+houseBusinessResultSet.getString("houseBId"));
                        return;
                    }


                    landEndTimeResultSet = landEndTimeStatement.executeQuery("SELECT * FROM HOUSE_OWNER_RECORD.BUSINESS_EMP WHERE (TYPE ='APPLY_EMP' OR TYPE='RECORD_EMP' ) and BUSINESS_ID='"+houseBusinessResultSet.getString("OID")+"'");
                    String task_id_sl=null;
                    if(landEndTimeResultSet.next()) {
                        if(landEndTimeResultSet.getString("TYPE").equals("APPLY_EMP")){
                            task_id_sl = landEndTimeResultSet.getString("ID");
                        }else if(landEndTimeResultSet.getString("TYPE").equals("RECORD_EMP")) {
                            task_id_sl = landEndTimeResultSet.getString("ID");
                        }else{
                            task_id_sl = null;
                        }
                    }

                    taskOperBusinessResultSet=taskOperBusinessStatement.executeQuery("select * from BUSINESS_FILE WHERE BUSINESS_ID ='"+houseBusinessResultSet.getString("OID")+"'");
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
                            businessFileWriter.write("(" + Q.v(Long.toString(bizFileId.getId()),Q.pm(taskOperBusinessResultSet.getString("NAME"))
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
                                            ,Long.toString(bizFileId.getId()),Q.p(workbookResultSet.getString("SIZE"))
                                            ,Q.pm(workbookResultSet.getString("MIME")),Q.p(workbookResultSet.getString("E_TAG"))
                                            ,Q.pm(order_num),Q.p(workbookResultSet.getString("FILE_NAME"))
                                            ,Q.p(task_id_sl)
                                    )+ ");");
                                }
                            }
                        }
                    }
                    businessFileWriter.flush();
                    j++;
                    System.out.println(j+"/"+String.valueOf(sumCount2));
                }
            }
        }catch (Exception e){

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
