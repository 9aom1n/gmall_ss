package com.gm.write;

import com.gm.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @Author: Gm
 * @Date: 2021/8/2 16:47
 */

public class Bulk_Write_WithJavaBean {
    public static void main(String[] args) throws IOException {
        //创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //批量写入数据
        Movie movie1004 = new Movie("1004", "星球大战");
        Movie movie1005 = new Movie("1005", "战狼");
        Movie movie1006 = new Movie("1006", "寻梦环游记");
        Index index1004 = new Index.Builder(movie1004).id("1004").build();
        Index index1005 = new Index.Builder(movie1005).id("1005").build();
        Index index1006 = new Index.Builder(movie1006).id("1006").build();

        Bulk bulk = new Bulk.Builder().addAction(index1004).addAction(index1005).addAction(index1006).defaultType("_doc").defaultIndex("movie_test1").build();

        jestClient.execute(bulk);
        //关闭连接

        jestClient.shutdownClient();

    }
}
