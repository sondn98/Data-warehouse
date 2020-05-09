package edu.hust.soict.bigdata.facilities.common.wal.impl.csv;

import com.opencsv.CSVReader;
import edu.hust.soict.bigdata.facilities.common.util.Reflects;
import edu.hust.soict.bigdata.facilities.common.wal.WalFile;
import edu.hust.soict.bigdata.facilities.common.wal.WalReader;
import edu.hust.soict.bigdata.facilities.common.wal.impl.json.JSONReader;
import edu.hust.soict.bigdata.facilities.model.DataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;

public class CSVWalReader<M extends DataModel> implements WalReader<M> {

    private BufferedReader reader;
    private static final Logger logger = LoggerFactory.getLogger(JSONReader.class);

    public CSVWalReader(WalFile file) throws FileNotFoundException {
        this.reader = new BufferedReader(new FileReader(file.absolutePath()));
    }

    @SuppressWarnings("unchecked")
    @Override
    public M next() {
        try(CSVReader csv = new CSVReader(reader)){
            String[] values = csv.readNext();
            Class<M> objClazz = (Class<M>)((ParameterizedType)this.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0];
            M obj = Reflects.newInstance(objClazz);
            Field[] fields = obj.getClass().getFields();
            for(int i = 0 ; i < fields.length ; i ++){
                Class type = fields[i].getType();
                try {
                    fields[i].set(obj, type.cast(values[i]));
                } catch (IllegalAccessException e) {
                    logger.warn("Can not set value for field " + fields[i].getName() + " while parsing " + objClazz.getName() + " from csv");
                }
            }

            return obj;
        } catch (IOException e) {
            logger.info(e.getMessage());
        }

        return null;
    }

    @Override
    public void close() {
        try {
            this.reader.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }
}
