package cn.edu.nju.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;

/**
 * Created by thpffcj on 2019/10/25.
 */
public class TagObject implements Serializable {

    @Autowired
    private String label;

    @Autowired
    private int value;

    public TagObject() {
    }

    public TagObject(String label, int value) {
        this.label = label;
        this.value = value;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
