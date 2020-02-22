package com.biubiu.engine;

public class ThreadResult {
    private Boolean code;
    private String msg;

    public ThreadResult(Boolean code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public Boolean getCode() {
        return code;
    }

    public void setCode(Boolean code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
