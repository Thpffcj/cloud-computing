package steamserverdemo;

import java.io.Serializable;
import java.util.ArrayList;

public class ApiReturnObject implements Serializable {

    public ApiReturnObject(ArrayList<TimeFieldObject> timeFieldObjects) {
        this.timeFieldObjects = timeFieldObjects;
    }

    private ArrayList<TimeFieldObject> timeFieldObjects;

    public ArrayList<TimeFieldObject> getTimeFieldObjects() {
        return timeFieldObjects;
    }

    public void setTimeFieldObjects(ArrayList<TimeFieldObject> timeFieldObjects) {
        this.timeFieldObjects = timeFieldObjects;
    }
}
