package edu.hust.soict.bigdata.facilities.common.exceptions;

/**
 * Exception xảy ra trong quá trình đọc ghi và xử lý file WAL
 *
 * @editor sondn on 2020/09/03
 */
public class WalException extends RuntimeException {

    public WalException(Throwable cause) {
        super(cause);
    }

    public WalException(String message, Throwable cause) {
        super(message, cause);
    }

}
