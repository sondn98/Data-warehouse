package edu.hust.soict.bigdata.facilities.common.exceptions;

/**
 * Exception xảy ra trong quá trình đọc ghi và xử lý file WAL
 *
 * @author <a href="https://github.com/tjeubaoit">tjeubaoit</a>
 * @editor sondn on 2020/09/03
 */
public class WalException extends CommonException {

    public WalException(String message, Throwable cause) {
        super(message, cause);
    }

    public WalException(Throwable cause) {
        super(cause);
    }

}
