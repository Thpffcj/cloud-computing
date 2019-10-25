package cn.edu.nju;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * Created by thpffcj on 2019/10/25.
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class TagReturnObject implements Serializable {

    private ArrayList<TagObject> tagObjects;
}
