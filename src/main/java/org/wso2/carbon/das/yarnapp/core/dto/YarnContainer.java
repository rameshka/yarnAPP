package org.wso2.carbon.das.yarnapp.core.dto;

import org.apache.hadoop.yarn.api.records.Container;

import java.util.List;

public class YarnContainer {
    private String execGroupName;
    private String parentAPPName;
    private List<String> siddhiAPPNameList;
    private Container container;

    public YarnContainer(List<String> siddhiAPPNameList, Container container) {
        this.siddhiAPPNameList = siddhiAPPNameList;
        this.container = container;
    }

    public List<String> getSiddhiAPPNameList() {
        return siddhiAPPNameList;
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

    public void addSiddhiAPPName(String appName){
        siddhiAPPNameList.add(appName);
    }

    public String getExecGroupName() {
        return execGroupName;
    }

    public void setExecGroupName(String execGroupName) {
        this.execGroupName = execGroupName;
    }
}
