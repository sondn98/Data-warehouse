package edu.hust.soict.bigdata.facilities.common.wal;

import edu.hust.soict.bigdata.facilities.model.DataModel;

/**
 * Dùng để write dữ liệu vào file WAL, cài đặt cụ thể tùy thuộc vào
 * codec của file và do các class implement lại interface này quyết định
 *
 * @author <a href="https://github.com/tjeubaoit">tjeubaoit</a>
 */
public interface WalWriter<M extends DataModel> extends AutoCloseable {

    /**
     * Write thêm dữ liệu vào cuối file, dữ liệu sẽ được write theo từng
     * block cho mỗi đoạn dữ liệu được thêm vào.
     *
     * @param data dữ liệu cần write
     */
    void append(M data);

    @Override
    void close();
}
