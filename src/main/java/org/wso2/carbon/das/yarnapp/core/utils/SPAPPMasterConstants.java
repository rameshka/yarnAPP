package org.wso2.carbon.das.yarnapp.core.utils;

public class SPAPPMasterConstants {

    public static final String SP_HDFS_NAME = "wso2sp.tar.gz";
    public static final Integer SP_PRIORITY_REQUIREMENT =0;
    public static final Integer CONTAINER_MEMORY = 256;
    public static final Integer SP_VCORE = 1; //the requrested vcore configuration is flexible as required vcore
    // configurations can be specified from yarn-site.xml
    public static final String SIDDHIAPP_HOLDER_HDFS_PATH ="siddhiappholderList.ser";
    public static final String SIDDHI_EXTENSION =".siddhi";
    public static final String SPAPP_MASTER = "SPAPPMaster.jar";
    public static final String SP_DEPLOYER_CLASS = "org.wso2.carbon.das.yarnapp.core.deployment.SPDeployer";
    public static final String SERIALIZED_FILE_EXTENSION =".ser";


}
