package org.wso2.carbon.das.yarnapp.core.pojo;

import org.apache.hadoop.yarn.api.records.Container;

import java.util.Map;

public class YarnContainer {
    private String parentAPPName;
    private Map<String,String> siddhiAppList;
    private Container container;

    public YarnContainer(Map<String,String> siddhiAppList, Container container) {
        this.siddhiAppList = siddhiAppList;
        this.container = container;
    }

    public Map<String,String> getSiddhiAppList() {
        return siddhiAppList;
    }

    public String getParentAPPName() {
        return parentAPPName;
    }

    public void setParentAPPName(String parentAPPName) {
        this.parentAPPName = parentAPPName;
    }

    public Container getContainer() {
        return container;
    }

    public void addSiddhiAPPName(String execGroupName,String appName){
        siddhiAppList.put(execGroupName,appName);
    }
}
