create or refresh streaming table silver_enriched 
comment "enriched table silver_table + id_lookup_table"
as select * except (i.id)
from stream(silver_table) as s
left join demodatabricks_7405619845701250.default.id_lookup_table as i 
on s.id=i.id 