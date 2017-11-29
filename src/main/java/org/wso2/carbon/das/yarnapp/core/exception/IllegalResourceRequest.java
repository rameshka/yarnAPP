package org.wso2.carbon.das.yarnapp.core.exception;

public class IllegalResourceRequest extends Exception{
    public IllegalResourceRequest(String s) {
        super(s);
    }

    public IllegalResourceRequest(String s, Throwable throwable) {
        super(s, throwable);
    }
}
