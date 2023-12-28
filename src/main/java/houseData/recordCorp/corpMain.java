package houseData.recordCorp;

import com.bean.JointCorpDevelop;
import com.mapper.JointCorpDevelopMapper;
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

public class corpMain {
    private static final String BEGIN_DATE = "2023-03-09";
    private static String DB_URL = "jdbc:mysql://127.0.0.1:3306/HOUSE_INFO?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull&transformedBitIsBoolean=true";
    private static final String CORP_FILE="/record_corp.sql";
    private static BufferedWriter cropWriter;
    private static File cropFileError;
    private static File cropFile;
    private static Statement cropStatement;
    private static ResultSet cropResultSet;

    public static void main(String agr[]) throws SQLException {

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
            //创建一个空开发商用于挂没有开发商的项目
            cropWriter.write("INSERT corp_snapshot (corp_name, tel, owner_name, owner_id_type, owner_id_number,address, unified_id, snapshot_id) VALUE ('未知开发商','13333333333','未知','RESIDENT_ID','未知','未知','011',0);");
            cropWriter.newLine();
            cropWriter.write("INSERT corp (unified_id, version, created_at,snapshot_id,updated_at) VALUE (0,0,'1990-01-01:08:00:00',0,'1990-01-01:08:00:00');");
            cropWriter.newLine();
            cropWriter.write("INSERT corp_record_snapshot (INFO_ID,  RECORD_ID, WORK_ID, SNAPSHOT_ID, TYPE) VALUE (0,0,0,0,'DEVELOPER');");

            cropWriter.newLine();
            cropWriter.write("INSERT corp_snapshot (corp_name, tel, owner_name, owner_id_type, owner_id_number,address, unified_id, snapshot_id) VALUE ('东港市房产测绘中心','13333333333','未知','RESIDENT_ID','未知','未知','012',1);");
            cropWriter.newLine();
            cropWriter.write("INSERT corp (unified_id, version, created_at,snapshot_id,updated_at) VALUE (1,0,'1990-01-01:08:00:00',1,'1990-01-01:08:00:00');");
            cropWriter.newLine();
            cropWriter.write("INSERT corp_record_snapshot (INFO_ID,  RECORD_ID, WORK_ID, SNAPSHOT_ID, TYPE) VALUE (1,1,1,1,'MAPPING');");

            cropWriter.newLine();
            cropWriter.write("INSERT corp_snapshot (corp_name, tel, owner_name, owner_id_type, owner_id_number,address, unified_id, snapshot_id) VALUE ('东港市村镇建设管理处测绘队','13333333333','未知','RESIDENT_ID','未知','未知','013',2);");
            cropWriter.newLine();
            cropWriter.write("INSERT corp (unified_id, version, created_at,snapshot_id,updated_at) VALUE (2,0,'1990-01-01:08:00:00',2,'1990-01-01:08:00:00');");
            cropWriter.newLine();
            cropWriter.write("INSERT corp_record_snapshot (INFO_ID,  RECORD_ID, WORK_ID, SNAPSHOT_ID, TYPE) VALUE (2,2,2,2,'MAPPING');");

            cropWriter.newLine();
            cropWriter.write("INSERT corp_snapshot (corp_name, tel, owner_name, owner_id_type, owner_id_number,address, unified_id, snapshot_id) VALUE ('未知测绘机构','13333333333','未知','RESIDENT_ID','未知','未知','014',3);");
            cropWriter.newLine();
            cropWriter.write("INSERT corp (unified_id, version, created_at,snapshot_id,updated_at) VALUE (3,0,'1990-01-01:08:00:00',3,'1990-01-01:08:00:00');");
            cropWriter.flush();
            cropWriter.newLine();
            cropWriter.write("INSERT corp_record_snapshot (INFO_ID,  RECORD_ID, WORK_ID, SNAPSHOT_ID, TYPE) VALUE (3,3,3,3,'MAPPING');");

        }catch (IOException e){
            System.out.println("cropWriter 文件创建失败");
            e.printStackTrace();
            return;
        }



        cropStatement = MyConnection.getStatement(DB_URL,"root","dgsoft");
        SqlSession sqlSession = MybatisUtils.getSqlSession();
        JointCorpDevelopMapper jointCorpDevelopMapper =  sqlSession.getMapper(JointCorpDevelopMapper.class);
        JointCorpDevelop jointCorpDevelop = null;
        String UNIFIED_ID=null;

        try {
            cropResultSet = cropStatement.executeQuery("select DEVELOPER.ID AS DID,DEVELOPER.*,ATTACH_CORPORATION.* from DEVELOPER LEFT JOIN ATTACH_CORPORATION ON DEVELOPER.ATTACH_ID = ATTACH_CORPORATION.ID ORDER BY ATTACH_CORPORATION.ID");//where DEVELOPER.ID='N2586'
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



                    cropWriter.write("INSERT corp_snapshot(CORP_NAME,TEL,OWNER_NAME,OWNER_ID_TYPE,OWNER_ID_NUMBER,ADDRESS,UNIFIED_ID,SNAPSHOT_ID,post_code,owner_tel,corp_id_type) VALUE ");
                    cropWriter.write("(" +Q.v(Q.pm(cropResultSet.getString("NAME")),Q.pm(cropResultSet.getString("PHONE"))
                            ,Q.pm(cropResultSet.getString("OWNER_NAME")),Q.pm("RESIDENT_ID")
                            ,Q.pm(cropResultSet.getString("OWNER_CARD")),Q.pm(cropResultSet.getString("ADDRESS"))
                            ,Q.pm(UNIFIED_ID),Long.toString(jointCorpDevelop.getCorpId())
                            ,Q.p(cropResultSet.getString("POST_CODE")),Q.p(cropResultSet.getString("owner_tel"))
                            ,Q.p("COMPANY")
                    )+ ");");


                    cropWriter.newLine();
                    cropWriter.write("INSERT corp (UNIFIED_ID,VERSION,UPDATED_AT,CREATED_AT,SNAPSHOT_ID) VALUE ");
                    cropWriter.write("(" +Q.v(Q.pm(UNIFIED_ID),"0"
                            ,Q.pm(cropResultSet.getTimestamp("CREATE_TIME")),Q.pm(cropResultSet.getTimestamp("CREATE_TIME"))
                            ,Long.toString(jointCorpDevelop.getCorpId())
                    )+ ");");


                    if(cropResultSet.getString("COMPANY_CER_CODE")!=null &&
                            !cropResultSet.getString("COMPANY_CER_CODE").equals("")){
//                        System.out.println(cropResultSet.getString("COMPANY_CER_CODE"));

                        cropWriter.newLine();
                        cropWriter.write("INSERT work (work_id, data_source, created_at, updated_at, work_name, status, validate_at, completed_at, version, define_id, process, type) value ");
                        cropWriter.write("(" + Q.v(Long.toString(jointCorpDevelop.getCorpId()),Q.pm("OLD")
                                ,Q.pm(cropResultSet.getString("CREATE_TIME")),Q.pm(cropResultSet.getString("CREATE_TIME"))
                                ,Q.pm("导入开发商机构备案"),Q.pm("COMPLETED")
                                ,Q.pm(cropResultSet.getString("CREATE_TIME")),Q.pm(cropResultSet.getString("CREATE_TIME"))
                                ,"0",Q.pm("func.corp.record.create")
                                ,"true",Q.pm("data")
                        )+ ");");

                        cropWriter.newLine();
                        cropWriter.write("INSERT work_operator (work_id, type, user_id, user_name, task_id,work_time) VALUE ");
                        cropWriter.write("(" + Q.v(Long.toString(jointCorpDevelop.getCorpId()),Q.pm("CREATE")
                                ,"0",Q.pm("root"),Long.toString(jointCorpDevelop.getCorpId())
                                ,Q.pm(cropResultSet.getString("CREATE_TIME"))
                        )+ ");");

                        cropWriter.newLine();
                        cropWriter.write("INSERT qualification_snapshot (qualification_id, level, level_number, expire_at, register_gov) VALUE ");
                        cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),"1"
                                ,Q.pm(cropResultSet.getString("COMPANY_CER_CODE")),Q.pm(cropResultSet.getTimestamp("CREATE_TIME"))
                                ,Q.pm("未知")
                        )+ ");");


                        cropWriter.newLine();
                        cropWriter.write("INSERT corp_record_snapshot (INFO_ID, QUALIFICATION_ID, RECORD_ID, WORK_ID, SNAPSHOT_ID, TYPE) VALUE ");
                        cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),Long.toString(jointCorpDevelop.getCorpId())
                                ,Long.toString(jointCorpDevelop.getCorpId()),Long.toString(jointCorpDevelop.getCorpId())
                                ,Long.toString(jointCorpDevelop.getCorpId()),Q.pm("DEVELOPER")
                        )+ ");");


                        cropWriter.newLine();
                        cropWriter.write("INSERT joint_corp (record_id, unified_id, type, enabled, version, updated_at, created_at, info_id, pin) VALUE ");
                        cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),Q.pm(UNIFIED_ID)
                                ,Q.pm("DEVELOPER"),"true","0",Q.pm(cropResultSet.getTimestamp("CREATE_TIME")),Q.pm(cropResultSet.getTimestamp("CREATE_TIME"))
                                ,Long.toString(jointCorpDevelop.getCorpId()),Q.pm(cropResultSet.getString("PYCODE"))
                        )+ ");");



                        cropWriter.newLine();
                        cropWriter.write("INSERT corp_business(business_id, work_id, before_info_id, info_id, type, unified_id, updated_at, work_type) VALUE ");
                        cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),Long.toString(jointCorpDevelop.getCorpId())
                                ,Long.toString(jointCorpDevelop.getCorpId()),Long.toString(jointCorpDevelop.getCorpId())
                                ,Q.pm("DEVELOPER"),Q.pm(UNIFIED_ID)
                                ,Q.pm(cropResultSet.getTimestamp("CREATE_TIME")),Q.pm("BUSINESS")

                        )+ ");");

                    }else{
                        cropWriter.newLine();
                        cropWriter.write("INSERT qualification_snapshot (qualification_id, level, level_number, expire_at, register_gov) VALUE ");
                        cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId()),"1"
                                ,Q.pm("未知"),Q.pm(cropResultSet.getTimestamp("CREATE_TIME"))
                                ,Q.pm("未知")
                        )+ ");");
                        cropWriter.newLine();
                        cropWriter.write("INSERT corp_record_snapshot (INFO_ID,  RECORD_ID, WORK_ID, SNAPSHOT_ID, TYPE) VALUE ");
                        cropWriter.write("(" +Q.v(Long.toString(jointCorpDevelop.getCorpId())
                                ,Long.toString(jointCorpDevelop.getCorpId()),Long.toString(jointCorpDevelop.getCorpId())
                                ,Long.toString(jointCorpDevelop.getCorpId()),Q.pm("DEVELOPER")
                        )+ ");");


                    }

                    cropWriter.flush();
                }else{

                    System.out.println("jointCorpDevelop表么有对应的ID");
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
