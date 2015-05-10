package com.gigaspaces.blobstore.data;

import com.gigaspaces.annotation.pojo.SpaceId;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by kobi on 2/9/14.
 */
public class Data implements Serializable {
    private String id;
    private Integer secondaryId;
    private Map<String, byte[]> data;

    public Data() {
    }

    public Data(String id) {
        this.id = id;
    }

    public Data(Integer secondaryId) {
        this.secondaryId = secondaryId;
    }

    @SpaceId
    public String getId(){
        return id;
    }

    public void setId(String id){
        this.id = id;
    }

    public Integer getSecondaryId() {
        return secondaryId;
    }

    public void setSecondaryId(Integer secondaryId) {
        this.secondaryId = secondaryId;
    }

    public Map<String, byte[]> getData(){
        return data;
    }

    public void setData(Map<String, byte[]> data){
        this.data = data;
    }
}
