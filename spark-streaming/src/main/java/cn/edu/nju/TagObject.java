package cn.edu.nju;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;

/**
 * Created by thpffcj on 2019/10/25.
 */
@NoArgsConstructor
@AllArgsConstructor
@Data
public class TagObject implements Serializable {

    @Autowired
    private String label;

    @Autowired
    private int value;
}
