package cn.edu.nju.api;

import cn.edu.nju.domain.TagObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by thpffcj on 2019/10/25.
 */
public class TagReturnObject implements Serializable {

    private ArrayList<TagObject> tagObjects;

    public TagReturnObject(ArrayList<TagObject> tagObjects) {
        this.tagObjects = tagObjects;
    }

    public ArrayList<TagObject> getTagObjects() {
        return tagObjects;
    }

    public void setTagObjects(ArrayList<TagObject> tagObjects) {
        this.tagObjects = tagObjects;
    }
}
