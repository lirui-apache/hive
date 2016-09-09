set hive.cbo.enable=false;

-- SORT_QUERY_RESULTS

explain select * from src where exists (select * from src1 where src1.key=src.key and src1.value=src.value) limit 10;

select * from src where exists (select * from src1 where src1.key=src.key and src1.value=src.value) limit 10;