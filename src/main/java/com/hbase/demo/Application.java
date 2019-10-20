package com.hbase.demo;

import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * @author apktool
 * @title com.hbase.demo.Application
 * @description Create a Non-web Application
 * @date 2019-09-30 20:31
 */

@SpringBootApplication
public class Application implements CommandLineRunner {
    @Setter(onMethod = @__({@Autowired}))
    private Demo demo;

    public static void main(String[] args) {
        new Application().start(args);
    }

    private void start(String[] args) {
        new SpringApplicationBuilder(Application.class)
            .web(WebApplicationType.NONE)
            .run(args);
    }

    @Override
    public void run(String... args) throws Exception {
        demo.start(args);
    }
}
