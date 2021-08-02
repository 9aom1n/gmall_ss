package com.gm.write;

import com.gm.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @Author: Gm
 * @Date: 2021/8/2 16:37
 */

public class Single_Write_WithJavaBean {
    public static void main(String[] args) throws IOException {
        //创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //写入数据
        Movie movie = new Movie("1003", "一路向西");
        Index index = new Index.Builder(movie).id("1003").index("movie_test1").type("_doc").build();
        jestClient.execute(index);

        //关闭连接
        jestClient.shutdownClient();
    }
}
