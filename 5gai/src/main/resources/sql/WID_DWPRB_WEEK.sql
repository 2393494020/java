use noce;
DROP TABLE IF EXISTS `wid_dwprb_week`;
CREATE EXTERNAL TABLE `wid_dwprb_week`(
    `city_id`                            int,
    `base_statn_id`                      int,
    `base_statn_name`                    string,
    `cell_id`                            int,
    `cell_name`                          string,
    `bs_vendor`                          string,
    `is_indoor`                          string,
    `band`                               string,
    `system_type_or_standard`            string,
    `region`                             string,
    `attribute`                          bigint,
    `nb_flag`                            tinyint,
    `band_width`                         int,
    `dwprbmin`                           float,
    `dwprbmax`                           float,
    `mon_dwprbmin`                       float,
    `mon_dwprbmax`                       float,
    `mon_dwprbidxmax`                    string,
    `mon_dwprbidxmin`                    string,
    `tues_dwprbmin`                      float,
    `tues_dwprbmax`                      float,
    `tues_dwprbidxmax`                   string,
    `tues_dwprbidxmin`                   string,
    `wed_dwprbmin`                       float,
    `wed_dwprbmax`                       float,
    `wed_dwprbidxmax`                    string,
    `wed_dwprbidxmin`                    string,
    `thur_dwprbmin`                      float,
    `thur_dwprbmax`                      float,
    `thur_dwprbidxmax`                   string,
    `thur_dwprbidxmin`                   string,
    `fri_dwprbmin`                       float,
    `fri_dwprbmax`                       float,
    `fri_dwprbidxmax`                    string,
    `fri_dwprbidxmin`                    string,
    `sat_dwprbmin`                       float,
    `sat_dwprbmax`                       float,
    `sat_dwprbidxmax`                    string,
    `sat_dwprbidxmin`                    string,
    `sun_dwprbmin`                       float,
    `sun_dwprbmax`                       float,
    `sun_dwprbidxmax`                    string,
    `sun_dwprbidxmin`                    string
)
PARTITIONED BY (`begin_end` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'='|', 'serialization.format'='|')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'hdfs://noce1/DATA/PUBLIC/WID/WID_DWPRB_WEEK'