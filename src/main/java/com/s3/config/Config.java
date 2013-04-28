package com.s3.config;

import java.io.IOException;

import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

@Configuration
@ComponentScan("com.s3.client")
public class Config
{
    @Bean
    public PropertyPlaceholderConfigurer propertyConfigurer() throws IOException
    {
        PropertyPlaceholderConfigurer props = new PropertyPlaceholderConfigurer();
        props.setLocations(new Resource[] {new ClassPathResource("s3.properties")});
        return props;
    }
}
