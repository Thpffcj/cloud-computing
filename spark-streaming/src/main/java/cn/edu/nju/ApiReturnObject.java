package cn.edu.nju;

import java.io.Serializable;
import java.util.ArrayList;

public class ApiReturnObject implements Serializable {

    private ArrayList<TimeFieldObject> timeFieldObjects;

    public ApiReturnObject(ArrayList<TimeFieldObject> timeFieldObjects) {
        this.timeFieldObjects = timeFieldObjects;
    }

    public ArrayList<TimeFieldObject> getTimeFieldObjects() {
        return timeFieldObjects;
    }

    public void setTimeFieldObjects(ArrayList<TimeFieldObject> timeFieldObjects) {
        this.timeFieldObjects = timeFieldObjects;
    }
}
