package com.github.mstawowiak.persistent.queue;

import com.github.mstawowiak.persistent.queue.exception.SerializationException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public interface Payload extends Serializable {

    default byte[] serialize() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutput out = null;

        try {
            out = new ObjectOutputStream(baos);
            out.writeObject(this);
            return baos.toByteArray();
        } catch (IOException ex) {
            throw new SerializationException("Cannot serialize payload", ex);
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException ex) { } //NOPMD - ignore close exception
        }
    }

    static <P extends Payload> P deserialize(byte[] data) {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = null;

        try {
            is = new ObjectInputStream(in);
            return (P) is.readObject();
        } catch (IOException | ClassNotFoundException ex) {
            throw new SerializationException("Cannot deserialize payload", ex);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException ex) { } //NOPMD - ignore close exception
        }
    }
}
