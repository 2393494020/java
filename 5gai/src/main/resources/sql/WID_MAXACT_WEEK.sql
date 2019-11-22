use noce;
DROP TABLE IF EXISTS `wid_maxact_week`;
CREATE EXTERNAL TABLE `wid_maxact_week`(
    `city_id`                             int,
    `base_statn_id`                       int,
    `base_statn_name`                     string,
    `cell_id`                             int,
    `cell_name`                           string,
    `bs_vendor`                           string,
    `is_indoor`                           string,
    `band`                                string,
    `system_type_or_standard`             string,
    `region`                              string,
    `attribute`                           bigint,
    `nb_flag`                             tinyint,
    `band_width`                          int,
    `maxactmin`                           float,
    `maxactmax`                           float,
    `mon_maxactmin`                       float,
    `mon_maxactmax`                       float,
    `mon_maxactidxmax`                    string,
    `mon_maxactidxmin`                    string,
    `tues_maxactmin`                      float,
    `tues_maxactmax`                      float,
    `tues_maxactidxmax`                   string,
    `tues_maxactidxmin`                   string,
    `wed_maxactmin`                       float,
    `wed_maxactmax`                       float,
    `wed_maxactidxmax`                    string,
    `wed_maxactidxmin`                    string,
    `thur_maxactmin`                      float,
    `thur_maxactmax`                      float,
    `thur_maxactidxmax`                   string,
    `thur_maxactidxmin`                   string,
    `fri_maxactmin`                       float,
    `fri_maxactmax`                       float,
    `fri_maxactidxmax`                    string,
    `fri_maxactidxmin`                    string,
    `sat_maxactmin`                       float,
    `sat_maxactmax`                       float,
    `sat_maxactidxmax`                    string,
    `sat_maxactidxmin`                    string,
    `sun_maxactmin`                       float,
    `sun_maxactmax`                       float,
    `sun_maxactidxmax`                    string,
    `sun_maxactidxmin`                    string
)
PARTITIONED BY (`begin_end` string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim'='|', 'serialization.format'='|')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 'hdfs://noce1/DATA/PUBLIC/WID/WID_MAXACT_WEEK'