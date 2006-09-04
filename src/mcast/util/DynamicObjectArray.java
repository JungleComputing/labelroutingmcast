package mcast.util;

public class DynamicObjectArray {

    private static final int DEFAULT_SIZE = 64;

    private Object[] objects;

    private int last = -1;

    public DynamicObjectArray() {
        this(DEFAULT_SIZE);
    }

    public DynamicObjectArray(int size) {
        objects = new Object[size];
    }

    private void resize(int minimumSize) {
        int newSize = objects.length;

        while (newSize <= minimumSize) {
            newSize *= 2;
        }

        Object[] tmp = new Object[newSize];
        System.arraycopy(objects, 0, tmp, 0, objects.length);
        objects = tmp;
    }

    public void put(int index, Object o) {
        if (index >= objects.length) {
            resize(index);
        }

        objects[index] = o;

        if (index > last) {
            last = index;
        }
    }

    public void remove(int index) {
        if (index > last) {
            System.err.println("illegal remove in DynamicObjectArray");
            return;
        }

        objects[index] = null;
        if (index == last) {
            while (index >= 0 && objects[index] == null) {
                index--;
            }
            last = index;
        }
    }

    public Object get(int index) {
        if (index < 0 || index > last) { 
            System.err.println("illegal get in DynamicObjectArray");
            return null;
        }

        return objects[index];
    }

    public int last() {
        return last;
    }
}
