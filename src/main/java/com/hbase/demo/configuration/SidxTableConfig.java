package com.hbase.demo.configuration;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;
import org.springframework.core.env.CompositePropertySource;
import org.springframework.core.io.support.DefaultPropertySourceFactory;
import org.springframework.core.io.support.EncodedResource;

import java.io.IOException;
import java.util.List;

/**
 * @author apktool
 * @title com.hbase.demo.configuration.SidxTableStruct
 * @description Build SidxTable config from struct.yml
 * @date 2019-10-02 15:00
 */
@PropertySources({
    @PropertySource(name = "yaml", value = "file:${user.dir}/config/struct.yml", ignoreResourceNotFound = true, encoding = "UTF-8", factory = SidxTableConfig.YamlPropertyLoaderFactory.class),
    @PropertySource(name = "yaml", value = "classpath:struct.yml", ignoreResourceNotFound = true, encoding = "UTF-8", factory = SidxTableConfig.YamlPropertyLoaderFactory.class)
})
@Configuration
@ConfigurationProperties("sidx")
@NoArgsConstructor
@Data
public class SidxTableConfig {
    /**
     * tableName : test
     * tableConfig : {"dataTableBlockSize":0,"dataTableBlockEncoding":"FAST_DIFF","dataTableCompression":"LZ4","dataTableRegions":32,"indexTableRegions":3}
     * tableColumns : [{"index":true,"qualifier":"c1","family":"f"},{"index":false,"qualifier":"c1","family":"f"}]
     */

    private String tableName;
    private TableConfig tableConfig;
    private List<TableColumn> tableColumns;

    /**
     * Qualifier Type
     *
     * @description These byte array are not equal for the same value from different type
     */
    @AllArgsConstructor
    public enum QualifierType {
        /**
         * Wrapper of Java Type
         */

        BYTE("java.lang.Byte"),
        SHORT("java.lang.Short"),
        INTEGER("java.lang.Integer"),
        LONG("java.lang.Long"),
        FLOAT("java.lang.Float"),
        DOUBLE("java.lang.Double"),
        BOOLEAN("java.lang.Boolean"),
        STRING("java.lang.String");

        @Getter
        private String typeClassName;
    }

    @NoArgsConstructor
    @Data
    public static class TableConfig {
        /**
         * dataTableBlockEncoding : FAST_DIFF
         * dataTableCompression : LZ4
         * dataTableBlockSize : 0
         * dataTableRegions : 32
         * indexTableRegions : 3
         */

        private String dataTableBlockEncoding;
        private String dataTableCompression;
        private int dataTableBlockSize;
        private int dataTableRegions;
        private String indexTableBlockEncoding;
        private String indexTableCompression;
        private int indexTableBlockSize;
        private int indexTableRegions;
    }

    @NoArgsConstructor
    @Data
    public static class TableColumn {
        /**
         * index : true
         * qualifier : c1
         * family : f
         * type: Integer
         */

        private boolean index;
        private String qualifier;
        private String family;
        private QualifierType type;
    }

    static class YamlPropertyLoaderFactory extends DefaultPropertySourceFactory {
        @Override
        public org.springframework.core.env.PropertySource<?> createPropertySource(String name, EncodedResource resource) throws IOException {
            if (resource == null || !resource.getResource().exists()) {
                return super.createPropertySource(name, resource);
            }

            CompositePropertySource propertySource = new CompositePropertySource(name);

            new YamlPropertySourceLoader()
                .load(resource.getResource().getFilename(), resource.getResource())
                .stream()
                .forEach(propertySource::addPropertySource);

            return propertySource;
        }
    }
}
