package edu.hust.soict.bigdata.facilities.common.wal;

import edu.hust.soict.bigdata.facilities.common.wal.impl.json.JSONReader;
import edu.hust.soict.bigdata.facilities.common.wal.impl.json.JSONWriter;
import edu.hust.soict.bigdata.facilities.common.wal.impl.object.ObjectReader;
import edu.hust.soict.bigdata.facilities.common.wal.impl.object.ObjectWriter;
import edu.hust.soict.bigdata.facilities.common.wal.types.IdGenerator;
import edu.hust.soict.bigdata.facilities.common.wal.types.SequenceIdGenerator;
import edu.hust.soict.bigdata.facilities.model.WalInfo;

import java.io.IOException;

/**
 * Interface biểu diễn cho một đối tượng file WAL
 *
 * @author <a href="https://github.com/tjeubaoit">tjeubaoit</a>
 */
public interface WalFile {
    /**
     * Dùng để generate suffix cho tên file WAL, dùng để tạo
     * tên file mới không trùng với bất kì file nào đang có
     */
    IdGenerator ID_GENERATOR = new SequenceIdGenerator();

    /**
     * @return Tên file wal
     */
    String name();

    /**
     * @return Codec cuar file Wal
     */
    String codec();

    /**
     * @return file size
     */
    Long length();

    /**
     * @return file information
     */
    WalInfo getInfo();

    /**
     * @return Đường dẫn tuyệt đối của File, có thể bao gồm cả URI
     */
    String absolutePath();

    /**
     * @return true nếu file tồn tại và false nếu không tồn tại
     */
    boolean exists();

    /**
     * @return true nếu file có độ lớn đã đạt tới giới hạn và falsen nếu chưa
     */
    boolean isReachedLimit();

    /**
     * Xóa file
     *
     * @return true nếu xóa file thành công hoặc false nếu có lỗi xảy ra
     */
    boolean delete();

    /**
     * @return new WAL file name
     */
    static String newFileName() {
        return "wal-" + ID_GENERATOR.generate() + ".log";
    }

}
