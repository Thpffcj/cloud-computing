package cn.edu.nju.domain;

import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;

public class GameObject implements Serializable{

    @Autowired
    private String id;
    @Autowired
    private String label;
    @Autowired
    private int value;
    @Autowired
    private String color;

    public GameObject(String id, String label, int value, String color){
        this.id = id;
        this.label = label;
        this.value = value;
        this.color = color;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
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

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }
}
