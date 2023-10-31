package houseData.recordCorp;

import com.bean.JointCorpDevelop;
import com.mapper.JointCorpDevelopMapper;
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

public class corpMain {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static final String CORP_ERROR_FILE="/corpError.sql";
    private static final String CORP_FILE="/corpRecord.sql";
    private static BufferedWriter cropWriterError;
    private static BufferedWriter cropWriter;
    private static File cropFileError;
    private static File cropFile;
    private static Statement cropStatement;
    private static ResultSet cropResultSet;

    public static void main(String agr[]) throws SQLException {
        cropFileError = new File(CORP_ERROR_FILE);
        if(cropFileError.exists()){
            cropFileError.delete();
        }
        cropFile = new File(CORP_FILE);
        if(cropFile.exists()){
            cropFile.delete();
        }

        try{
            cropFile.createNewFile();
            FileWriter fw = new FileWriter(cropFile.getAbsoluteFile());
            cropWriter = new BufferedWriter(fw);
            cropWriter.write("USE record_corp;");
            cropWriter.newLine();
            cropWriter.write("INSERT work.work_define (define_id, work_name, process, enabled, version, type) VALUE ('func.corp.record.import','从业机构导入',false,true,0,'data');");
            cropWriter.newLine();
            cropWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) "
            +"value (0,'OLD','2023-10-28 18:30:45','2023-10-28 18:30:45','从业机构导入','COMPLETED','2023-10-28 18:30:45','2023-10-28 18:30:45',0,'func.corp.record.create',false,'data');");
            cropWriter.newLine();
            cropWriter.write("INSERT work_operator (work_id, type, user_id, user_name, org_name, task_id) "
                    +"VALUE (0,'CREATE','0','root','从业机构导入','wxy');");
            cropWriter.newLine();
            cropWriter.write("INSERT work_task (TASK_ID, MESSAGE, TASK_NAME, PASS) "
                    +"VALUE ('wxy','从业机构导入','从业机构导入',true);");
            cropWriter.flush();
        }catch (IOException e){
            System.out.println("cropWriter 文件创建失败");
            e.printStackTrace();
            return;
        }


        try{
            cropFileError.createNewFile();
            FileWriter fw = new FileWriter(cropFileError.getAbsoluteFile());
            cropWriterError = new BufferedWriter(fw);
            cropWriterError.write("corp--错误记录:");
            cropWriterError.newLine();
            cropWriterError.flush();
        }catch (IOException e){
            System.out.println("cropWriterError 文件创建失败");
            e.printStackTrace();
            return;
        }
        cropStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        JointCorpDevelop jointCorpDevelop = null;
        String UNIFIED_ID=null;

        try {
            cropResultSet = cropStatement.executeQuery("select DEVELOPER.ID AS DID,DEVELOPER.*,ATTACH_CORPORATION.* from DEVELOPER LEFT JOIN ATTACH_CORPORATION ON DEVELOPER.ATTACH_ID = ATTACH_CORPORATION.ID");
            cropResultSet.last();
            int sumCount = cropResultSet.getRow(),i=0;
            System.out.println("记录总数-"+sumCount);
            cropResultSet.beforeFirst();
            while (cropResultSet.next()){
                jointCorpDevelop = jointCorpDevelopMapper.selectByDevelopId(cropResultSet.getString("DID"));
                if (jointCorpDevelop!=null && jointCorpDevelop.getDeveloperId()!=null && !jointCorpDevelop.getDeveloperId().equals("")){

                    cropWriter.newLine();
                    UNIFIED_ID = cropResultSet.getString("LICENSE_NUMBER");
                    if (UNIFIED_ID==null || UNIFIED_ID.isBlank()){
                        UNIFIED_ID = Long.toString(jointCorpDevelop.getCorpId());
                    }
                    cropWriter.write("INSERT corp_snapshot(CORP_NAME, TEL, OWNER_NAME, OWNER_ID_TYPE, OWNER_ID_NUMBER, ADDRESS, UNIFIED_ID, SNAPSHOT_ID) VALUE ");
                    cropWriter.write("(" +Q.v(Q.pm(cropResultSet.getString("NAME")),Q.pm(cropResultSet.getString("PHONE"))
                            ,Q.pm(cropResultSet.getString("OWNER_NAME")),Q.pm(cropResultSet.getString("CREDENTIALS_TYPE"))
                            ,Q.pm(cropResultSet.getString("COMPANY_CER_CODE")),Q.pm(cropResultSet.getString("ADDRESS"))
                            ,Q.pm(UNIFIED_ID),Long.toString(jointCorpDevelop.getCorpId())
                    )+ ");");

                    cropWriter.newLine();
                    cropWriter.write("INSERT corp (UNIFIED_ID, VERSION, UPDATED_AT, CREATED_AT, SNAPSHOT_ID) VALUE ");
                    cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),"0"
                            ,Q.pm("2023-10-28 18:30:45"),Q.pm("2023-10-28 18:30:45")
                            ,Long.toString(jointCorpDevelop.getCorpId())
                    )+ ");");



                    cropWriter.newLine();
                    cropWriter.write("INSERT qualification_snapshot (QUALIFICATION_ID, LEVEL, LEVEL_NUMBER, EXPIRE_AT, REGISTER_GOV) VALUE ");
                    cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),"0"
                            ,Q.pm(cropResultSet.getString("COMPANY_CER_CODE")),Q.pm("2023-10-28 18:30:45"),Q.pm("未知")
                    )+ ");");

                    cropWriter.newLine();
                    cropWriter.write("INSERT corp_record_snapshot (INFO_ID, QUALIFICATION_ID, RECORD_ID, WORK_ID, SNAPSHOT_ID, TYPE) VALUE ");
                    cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),Long.toString(jointCorpDevelop.getCorpId())
                            ,Long.toString(jointCorpDevelop.getCorpId()),"0",Long.toString(jointCorpDevelop.getCorpId())
                            ,Q.pm("DEVELOPER")
                    )+ ");");

                    cropWriter.newLine();
                    cropWriter.write("INSERT joint_corp (record_id, unified_id, type, enabled, version, updated_at, created_at, info_id, pin) VALUE ");
                    cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),Long.toString(jointCorpDevelop.getCorpId())
                            ,Q.pm("DEVELOPER"),"true","0",Q.pm("2023-10-28 18:30:45"),Q.pm("2023-10-28 18:30:45")
                            ,Long.toString(jointCorpDevelop.getCorpId()),Q.pm(cropResultSet.getString("PYCODE"))
                    )+ ");");

                    cropWriter.newLine();
                    cropWriter.write("INSERT corp_business(business_id, work_id, before_info_id, info_id, type, unified_id, updated_at, work_type) VALUE ");
                    cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),"0"
                            ,Long.toString(jointCorpDevelop.getCorpId()),Long.toString(jointCorpDevelop.getCorpId())
                            ,Q.pm("DEVELOPER"),Long.toString(jointCorpDevelop.getCorpId())
                            ,Q.pm("2023-10-28 18:30:45"),Q.pm("BUSINESS")

                    )+ ");");

                    cropWriter.flush();
                }else{
                    cropWriterError.newLine();
                    cropWriterError.write("没有找到对应记录检查jointCorpDevelop--:"+cropResultSet.getString("DID"));
                    cropWriterError.flush();
                }







                i++;
                System.out.println(i+"/"+String.valueOf(sumCount));
            }


        }catch (Exception e){
            System.out.println("id is errer-----id:"+cropResultSet.getString("DID"));
            e.printStackTrace();
        }finally {

            if (cropResultSet!=null){
                cropResultSet.close();
            }
            if(cropResultSet!=null){
                cropResultSet.close();
            }
            sqlSession.close();
            MyConnection.closeConnection();

        }

    }


}
