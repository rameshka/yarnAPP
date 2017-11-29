package org.wso2.carbon.das.yarnapp.core.deployment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;
import org.wso2.carbon.das.jobmanager.core.model.SiddhiAppHolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

import static org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants.SERIALIZED_FILE_EXTENSION;
import static org.wso2.carbon.das.yarnapp.core.utils.SPAPPMasterConstants.SIDDHI_EXTENSION;

/**
 * This class will be executed inside requested containers which will create assign set of SiddhiApps inside container
 */

public class SPDeployer {
    private static final Logger LOG = Logger.getLogger(SPDeployer.class);
    private Configuration configuration;
    private String streamProcessorUnzipedPath;
    private String parentAppName;

    public static void main(String[] args) {
        SPDeployer spDeployer = new SPDeployer(args[0], args[1]);
        try {
            for (int i = 2; i < args.length; i++) {
                System.out.println(args[i]);
                spDeployer.createSiddhiAPP(args[i]);
            }
        } catch (IOException e) {
            LOG.error("Unhandled IOexception", e);
        } catch (ClassNotFoundException e) {
            LOG.error("Error encountered while Deserialization", e);
        }


    }

    public SPDeployer(String streamProcessorUnzipedPath, String parentAppName) {
        this.configuration = new YarnConfiguration();
        this.streamProcessorUnzipedPath = streamProcessorUnzipedPath;
        this.parentAppName = parentAppName;
    }

    /**
     * This method is used to locate the serialized siddhiApps written in to the HDFS
     * deserialize them and allocate the .siddhi files inside container.
     *
     * @param fileName
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void createSiddhiAPP(String fileName) throws IOException, ClassNotFoundException {

        FileSystem fs = FileSystem.get(configuration);

        //path to locate the file from HDFS
        String hdfsReadPath = fs.getHomeDirectory().toUri()
                            + File.separator
                            + parentAppName
                            + File.separator
                            + fileName
                            + SERIALIZED_FILE_EXTENSION;

        InputStream in = fs.open(new Path(hdfsReadPath));
        ObjectInputStream objReader = new ObjectInputStream(in);

        //Path to write the file
        String siddhiAppPath =  streamProcessorUnzipedPath
                                + File.separator
                                + "deployment"
                                + File.separator
                                + "siddhi-files"
                                + File.separator
                                + fileName
                                + SIDDHI_EXTENSION;

        File file = new File(siddhiAppPath);
        if (!file.exists()) {
            file.createNewFile();
        }

        SiddhiAppHolder siddhiAppHolder = (SiddhiAppHolder) objReader.readObject();
        FileWriter fw = new FileWriter(file);
        fw.write(siddhiAppHolder.getSiddhiApp());
        fw.close();
        objReader.close();
    }
}