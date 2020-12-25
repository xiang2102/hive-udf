

```
DROP  FUNCTION if exists count_bit_map_distinct;
add jar ${home}/lib/hive-udf.jar;

create function count_bit_map_distinct as 'sao.so.hive.util.udaf.RoaringBitMapCountUDAF';
select product_id,count_bit_map_distinct(scan_time_unix) from top_dw.fact_query_record_add_ip group by product_id;

```

