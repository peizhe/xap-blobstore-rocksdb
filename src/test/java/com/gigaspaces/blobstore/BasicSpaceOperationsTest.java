package com.gigaspaces.blobstore;

import com.gigaspaces.blobstore.data.Data;
import com.j_spaces.core.LeaseContext;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openspaces.core.GigaSpace;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kobi on 2/10/14.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations={"context.xml"})
public class BasicSpaceOperationsTest {

    @Resource(name = "gigaSpace")
    protected GigaSpace gigaSpace;


    @Test
    public void test() throws InterruptedException {
        Thread.sleep(30000);
        System.out.println("After first sleep");
        final List<Data> list = new ArrayList<Data>();

        for(int i = 0; i < 100; i++){
            Data data = new Data();
            data.setId(String.valueOf(i));
            Map<String, byte[]> map = new HashMap<String, byte[]>();
            byte[] bytes = new byte[300];
            for(int j = 0; j < 300; j++){
                bytes[j] = 1;
            }
            map.put("0", bytes);
            data.setData(map);
            if(i % 2 ==0)
                data.setSecondaryId(1);
            else
                data.setSecondaryId(2);
            list.add(data);
        }


        LeaseContext<Object>[] leaseContext = gigaSpace.writeMultiple(list.toArray());

        Data data1 = new Data();
        data1.setSecondaryId(2);
        Data[] datas = gigaSpace.takeMultiple(data1);
        Thread.sleep(30000);
        System.out.println("DONE");
    }
}
