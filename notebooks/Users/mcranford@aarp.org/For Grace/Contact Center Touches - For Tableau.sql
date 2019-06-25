-- Databricks notebook source
--QC total number of contacts for a certain time period;

select count(*) from default.f_communication
where received_dt >= '2017-01-01';

-- COMMAND ----------

--This is joining 4 tables into an output dataset for contact center touches;
drop table if exists temp.mike_contact_center_touches;

create table temp.mike_contact_center_touches as
select  
a.acct_nbr, a.person_id, a.received_dt, a.comm_method, a.subject_desc, a.topic_desc,
b.age_agg_ind, b.diversity_fl_agg_ind, b.register_ind, b.hid_key, b.cid_key, b.mid_key,
c.account_stat, c.kx_create_dt, c.orig_key_cd, c.term_agg_act, c.times_renewed_agg_act,
d.city, d.state, d.zip

from default.f_communication a

left join default.d_individual b
on a.person_id = b.cid_key
and a.acct_nbr = b.chid_agg_ind
and b.is_current = 'Y' and b.is_deleted = 'N'


left join default.d_account c
on a.acct_nbr = c.chid_key
and c.is_current = 'Y' and c.is_deleted = 'N'

left join default.d_household d
on b.hid_key = d.hid_key
and d.is_current = 'Y'

where a.received_dt >= '2017-01-01';


select count(*) from temp.mike_contact_center_touches;

-- COMMAND ----------

select comm_method, count(*) as total_touches
from temp.mike_contact_center_touches
group by comm_method
order by total_touches desc;

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val new_table = sqlContext.read.table("temp.mike_contact_center_touches");

-- COMMAND ----------

-- MAGIC %run /Users/production@aarp.com/Utils/user_env_variables_scala

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC val url = s"jdbc:redshift://$rs_host:$rs_port/$rs_dbname?user=mcranford&password=Potatoes@123"
-- MAGIC val tempdir_url = s"s3n://$userAccessKey:$userSecretKey@aarp-archive/temp/"
-- MAGIC import sqlContext.implicits._

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC new_table.coalesce(10).write 
-- MAGIC   .format("com.databricks.spark.redshift") 
-- MAGIC   .option("url", url) 
-- MAGIC   .option("dbtable", "temp.mike_contact_center_touches") 
-- MAGIC   .option("aws_iam_role","arn:aws:iam::148546933577:role/CasertaRedshiftCopy")
-- MAGIC   .option("tempdir", tempdir_url) 
-- MAGIC   .mode("overwrite") 
-- MAGIC   .save()