package core;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;

/**
 * Created by shanmukh on 11/15/15.
 */
public class ObjectSerializer {
    Kryo kryo = new Kryo();

    public void serializeObject(Object obj, String fileName) throws Exception {
        File f = new File(fileName);
        if (f.exists()) {
            f.delete();
        }
        try {

            FileOutputStream fileOut =
                    new FileOutputStream(fileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(obj);
            out.close();
            fileOut.close();

        } catch (Exception e) {
            if (f.exists()) {
                f.delete();
            }
        }
    }

    public Object readObject(String fileName) throws Exception {
        FileInputStream fileIn = new FileInputStream(fileName);
        ObjectInputStream in = new ObjectInputStream(fileIn);
        Object obj = in.readObject();
        in.close();
        fileIn.close();
        return obj;
    }

    public boolean canDeserialize(String fileName) throws Exception {
        try {
            FileInputStream fileIn = new FileInputStream(fileName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            Object obj = in.readObject();
            in.close();
            fileIn.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
