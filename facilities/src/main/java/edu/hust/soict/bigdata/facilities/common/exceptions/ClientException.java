package edu.hust.soict.bigdata.facilities.common.exceptions;


public class ClientException extends Exception {

    public ClientException() {
        super();
    }

    public ClientException(String msg) {
        super(msg);
    }

    public ClientException(String msg, Exception e) {
        super(msg, e);
    }
}