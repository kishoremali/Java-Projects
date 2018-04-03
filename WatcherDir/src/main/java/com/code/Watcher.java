package com.code;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.file.StandardWatchEventKinds.*;

public class Watcher {
    static String watchDir = "";
    static String destDir = "";
    static String url = "";
    static String user_name = "";
    static String password = "";
    static String schName = "";
    static String tabName = "";

    static Logger log = Logger.getLogger(Watcher.class.getName());


    public static void main(String args[]){

        Properties prop = new Properties();
        InputStream propInput = null;
        //String conf_path = args[0];
        String conf_path = "/home/kishor/Imp/IDE/Workspace_IDEA/WatcherDir/config/";
        String log_conf_path = conf_path + "log4j.properties";

        try {
            PropertyConfigurator.configure(new FileInputStream(log_conf_path));
            propInput = new FileInputStream(conf_path + "config.cfg");
            prop.load(propInput);
        // Read config properties
            watchDir = prop.getProperty("watchdir");
            destDir = prop.getProperty("destdir");
            url = prop.getProperty("url");
            user_name = prop.getProperty("user_name");
            password = prop.getProperty("password");
            schName = prop.getProperty("schName");
            tabName= prop.getProperty("tabName");



            log.info("Welcome To Monitoring Directory Application");
            log.info("Watching Dir :"+watchDir);
            WatchService watcher = FileSystems.getDefault().newWatchService();
            Paths.get(watchDir).register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);

            while (true) {
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException ex) {
                    log.error("Exception while getting WatchKey:" + ex.getMessage());
                    return;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path fileName = ev.context();
                    String sourcefile = watchDir+File.separator+fileName;
                    String targetfile = destDir+File.separator+fileName;

                    if (kind == ENTRY_CREATE) {

                        log.info(kind+" Event triggered for file: "+fileName.toString());
                        log.info("Creating file: " + fileName.toString());

			// waiti till file will not complete copy into watcher directory

                        boolean isGrowing = false;
                        Long initialWeight = new Long(0);
                        Long finalWeight = new Long(0);
                        do {
                            initialWeight = Paths.get(sourcefile).toFile().length();
                            try {
				    // sleep for 1 second
                                Thread.sleep(1000);
                            }catch (InterruptedException e){
                                log.error("Exception while Thread sleep :"+e.getMessage());
                                return;
                            }
                            finalWeight = Paths.get(sourcefile).toFile().length();
                            log.info("File Growing from "+initialWeight+ " to "+finalWeight);
                            isGrowing = initialWeight < finalWeight;

                        } while(isGrowing);

                        log.info("Finished Growing/creating file!");
			
			// Move file from source to destination dir

                        log.info("Moving File :"+Paths.get(sourcefile));
                        Path p =Files.move( Paths.get(sourcefile), Paths.get(targetfile), StandardCopyOption.REPLACE_EXISTING);
                        log.info("Successfully moved File To :"+Paths.get(targetfile));


                        // get vertica connection
                        Connection connection = DriverManager.getConnection(url, user_name, password);
                        connection.setAutoCommit(false);

                        insert_vertica(schName,tabName,connection,targetfile);

                        connection.commit();
                        connection.close();

                    }
                    if (kind == ENTRY_DELETE) {
                        log.info(kind+" Event triggered for file: "+fileName.toString());
                        //System.out.println("Delete: " + fileName.toString());
                    }
                    if (kind == ENTRY_MODIFY) {
                        log.info(kind+" Event triggered for file: "+fileName.toString());
                        //System.out.println("Modify: " + fileName.toString());
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    break;
                }

            }
        }catch (Exception ex) {
            log.error("IO Exception:"+ex);
            //System.err.println(ex);
        }

    }
    public static  void insert_vertica(String schName,String tabName,Connection fconn,String filename){
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar calobj = Calendar.getInstance();
        String currdate= df.format(calobj.getTime());

        try {
            Statement stmt = fconn.createStatement();

            String qu_create_sch="CREATE SCHEMA  IF NOT EXISTS "+schName;
            String qu_create_tbl ="CREATE TABLE IF NOT EXISTS "+schName+"."+tabName+"(date timestamp,filename varchar(1024));";

            String qu_insert = "insert into "+schName+"."+tabName+"(date,filename) VALUES('"+currdate+"','"+filename+"');";

            stmt.execute(qu_create_sch);
            stmt.execute(qu_create_tbl);
            stmt.execute(qu_insert);
            stmt.close();
            log.info("Successfully executed: "+qu_insert);
        }
        catch (Exception ex){
            log.error(ex.getMessage());
            //System.err.print(ex);
        }
    }
}
