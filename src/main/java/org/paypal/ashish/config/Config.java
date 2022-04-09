package org.paypal.ashish.config;

import java.io.*;
import java.util.Properties;

public class Config {

    public  Properties properties = new Properties();
    public void loadPropertiesFile(){
        InputStream iStream = null;
        try {
            // Loading properties file from the path (relative path given here)
            iStream = new FileInputStream("src\\main\\resources\\config.properties");
            properties.load(iStream);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            try {
                if(iStream != null){
                    iStream.close();
                }
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

}
