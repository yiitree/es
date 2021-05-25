package com.es.es.utils;

import com.es.es.domain.Content;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: 曾睿
 * @Date: 2021/5/25 13:13
 */
public class HtmlParseUtil {

    public static void main(String[] args) throws IOException {

        new HtmlParseUtil().parseJd("java").forEach(System.out::println);

    }

    public List<Content> parseJd(String keywords) throws IOException{
        String url = "https://search.jd.com/Search?keyword="+keywords+"&enc=utf-8";

        Document document = Jsoup.parse(new URL(url), 30000);

        Element element = document.getElementById("J_goodsList");
        // 获取所有的li元素
        Elements elements = element.getElementsByTag("li");


        ArrayList<Content> goodsList = new ArrayList<>();
        for (Element el : elements) {
            // 图片地址 --- 关于图片比较多的图片，都是懒加载的
            String img = el.getElementsByTag("img").eq(0).attr("data-lazy-img");
            // 价格
            String price = el.getElementsByClass("p-price").eq(0).text();
            // 标题
            String title = el.getElementsByClass("p-name").eq(0).text();
            goodsList.add(new Content(title,price,img));
        }
        return goodsList;
    }

}
