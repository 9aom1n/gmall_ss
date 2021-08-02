package com.gm.write;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @Author: Gm
 * @Date: 2021/8/2 14:41
 */

public class Single_write {
    public static void main(String[] args) throws IOException {
        //创建客户端工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //设置连接属性
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

        //获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //构建ES插入数据对象
        Index index = new Index.Builder("{\n" +
                "  \"id\":\"1002\",\n" +
                "  \"movie_name\":\"复联\"\n" +
                "}").index("movie_test1").type("_doc").id("1002").build();

        //写入数据
        jestClient.execute(index);


        //关闭连接
        jestClient.shutdownClient();
    }
}
