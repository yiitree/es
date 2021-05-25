package com.es.es.controller;

import com.es.es.service.ContentService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
public class ContentController {
    @Autowired
    private ContentService contentService;

    @GetMapping("/parse/{keywords}")
    public Boolean parse(@PathVariable("keywords")String keywords) throws IOException{
        return contentService.parseContent(keywords);
    }

    @GetMapping("/search/{pageNo}/{pageSize}/{keywords}")
    public List<Map<String,Object>> search(
            @PathVariable("pageNo")int pageNo,
            @PathVariable("pageSize")int pageSize,
            @PathVariable("keywords")String keywords) throws IOException{
        return contentService.searchPage(pageNo,pageSize,keywords);
    }

    @GetMapping("/searchGl/{pageNo}/{pageSize}/{keywords}")
    public List<Map<String,Object>> searchGl(
            @PathVariable("pageNo")int pageNo,
            @PathVariable("pageSize")int pageSize,
            @PathVariable("keywords")String keywords) throws IOException{
        return contentService.searchPageGl(pageNo,pageSize,keywords);
    }
}
