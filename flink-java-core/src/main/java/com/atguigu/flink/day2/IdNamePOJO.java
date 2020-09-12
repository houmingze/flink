package com.atguigu.flink.day2;

/**
 * @author ：hmz
 * @date ：Created in 2020/9/11 15:51
 */
public class IdNamePOJO {

    private Integer id;
    private String name;

    public IdNamePOJO() {
    }

    public IdNamePOJO(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
