package houseData.recordBuild;

import com.bean.HouseId;
import com.bean.OwnerRecordHouseId;
import com.mapper.HouseIdMapper;
import com.mapper.OwnerRecordHouseIdMapper;
import com.utils.MyConnection;
import com.utils.MybatisUtils;
import org.apache.ibatis.session.SqlSession;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class updateCreateTime {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_OWNER_RECORD?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private final static String USER ="root";
    private final static String PASSWORD ="dgsoft";
    private static final String PROJECT_FILE="/updateCreateTime.sql";
    private static File projectBusinessFile;
    private static BufferedWriter projectBusinessWriter;

    private static Statement taskOperBusinessStatement;
    private static ResultSet taskOperBusinessResultSet;

    private static Statement workbookStatement;
    private static ResultSet workbookResultSet;

    private static Statement houseStatement;
    private static ResultSet houseResultSet;

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
            projectBusinessWriter.flush();
        }catch (IOException e){
            System.out.println("projectBusinessWriter 文件创建失败");
            e.printStackTrace();
            return;
        }

        houseStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        workbookStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);
        taskOperBusinessStatement = MyConnection.getStatement(DB_URL,USER,PASSWORD);



        try {
            houseResultSet = houseStatement.executeQuery("select O.CREATE_TIME,O.APPLY_TIME,O.REG_TIME,O.RECORD_TIME,BH.AFTER_HOUSE,w.work_id from HOUSE_OWNER_RECORD.OWNER_BUSINESS as O LEFT JOIN HOUSE_OWNER_RECORD.BUSINESS_HOUSE AS BH ON O.ID=BH.BUSINESS_ID " +
                    "LEFT JOIN INTEGRATION.ownerRecordHouseId AS OHI ON BH.AFTER_HOUSE=OHI.oid " +
                    "left join record_building.work as w on OHI.id=w.work_id " +
                    "where O.STATUS IN('COMPLETE','COMPLETE_CANCEL','MODIFYING') AND O.DEFINE_ID IN('WP42','BL42','WP43','WP40') " +
                    "and w.work_id is NOT null");
            houseResultSet.last();
            int sumCount = houseResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            houseResultSet.beforeFirst();
            while (houseResultSet.next()){

                projectBusinessWriter.newLine();
                projectBusinessWriter.write("update record_building.work set created_at='"+houseResultSet.getString("APPLY_TIME")+"',updated_at='"+houseResultSet.getString("APPLY_TIME")+"',validate_at='"+houseResultSet.getString("REG_TIME")+"',completed_at='"+houseResultSet.getString("RECORD_TIME")+"' where work_id="+houseResultSet.getInt("work_id")+";");
                projectBusinessWriter.flush();

                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }

        }catch (Exception e){
            e.printStackTrace();
            return;
        }finally {

        }
    }
}
