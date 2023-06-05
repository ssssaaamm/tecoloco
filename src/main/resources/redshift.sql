CREATE TABLE dim_time (
	id_dim_time varchar(8) not null primary key
	,date date not null
	,year int not null
	,month int not null
	,day int not null
);
comment on column dim_time.id_dim_time is 'Primary key of this dimension. Surrogate key';
comment on column dim_time.date is 'Date in format yyyy-MM-dd';
comment on column dim_time.year is 'Year of the date value';
comment on column dim_time.month is 'Month of the date value';
comment on column dim_time.day is 'Day of the date value';

create table dim_company(
	id_dim_company varchar(50) not null primary key
	,company varchar(100) not null
	,sector varchar(50) not null
	,city varchar(50) not null
);
comment on column dim_company.id_dim_company is 'Primary key of this dimension. Surrogate key';
comment on column dim_company.company is 'Name of the company';
comment on column dim_company.sector is 'Sector the company belongs to';
comment on column dim_company.city is 'City where the company is located at';

create table dim_job_info(
	id_dim_job_info varchar(50) not null primary key
	,job_title varchar(100) not null
	,benefits jsonb not null
);
comment on column dim_job_info.id_dim_job_info is 'Primary key of this dimension. Surrogate key';
comment on column dim_job_info.job_title is 'Job title';
comment on column dim_job_info.benefits is 'Array of benefits offered by the company in the job';

create table dim_job_post_status(
	id_dim_job_post_status varchar(50) not null primary key
	,post_status varchar(10) not null
);
comment on column dim_job_post_status.id_dim_job_post_status is 'Primary key of this dimension. Surrogate key';
comment on column dim_job_post_status.post_status is 'Post status (Active, Inactive or Deleted)';

create table dim_applicant(
	id_dim_applicant varchar(50) not null primary key
	,first_name varchar(50) not null
	,last_name varchar(50) not null
	,full_name varchar(100) not null
	,age int not null
	,skills jsonb not null
);
comment on column dim_applicant.id_dim_applicant is 'Primary key of this dimension. Surrogate key';
comment on column dim_applicant.first_name is 'First name of the applicant';
comment on column dim_applicant.last_name is 'Last name of the applicant';
comment on column dim_applicant.full_name is 'Concatenation of first_name and last_name';
comment on column dim_applicant.age is 'Age of the applicant';
comment on column dim_applicant.skills is 'Array of skills that the applicant has';


create table fact_job_post(
	id_fact_job_post varchar(50) not null primary key
	,job_id varchar(50) not null --degenerate dim
	,id_dim_time varchar(8) not null
	,id_dim_company varchar(50) not null
	,id_dim_job_info varchar(50) not null
	,id_dim_job_post_status varchar(50) not null
	,active_days int not null --metric
	,unique( job_id, id_dim_time, id_dim_company, id_dim_job_info, id_dim_job_post_status)
	,constraint fk_time foreign key(id_dim_time) references dim_time(id_dim_time)
	,constraint fk_company foreign key(id_dim_company) references dim_company(id_dim_company)
	,constraint fk_job_info foreign key(id_dim_job_info) references dim_job_info(id_dim_job_info)
	,constraint fk_job_post_status foreign key(id_dim_job_post_status) references dim_job_post_status(id_dim_job_post_status)
);
comment on column fact_job_post.id_fact_job_post is 'Primary key of this fact table. Surrogate key';
comment on column fact_job_post.job_id is 'Job Id. Degenerate Dimension';
comment on column fact_job_post.id_dim_time is 'Foreign key to time dimension';
comment on column fact_job_post.id_dim_company is 'Foreign key to company dimension';
comment on column fact_job_post.id_dim_job_info is 'Foreign key to job info dimension';
comment on column fact_job_post.id_dim_job_post_status is 'Foreign key to job post status dimension';
comment on column fact_job_post.active_days is 'Number of days the job post was active';


create table fact_job_applicant(
	id_fact_applicant varchar(50) not null primary key
	,job_id varchar(50) not null --degenerate dim
	,id_dim_time varchar(8) not null
	,id_dim_company varchar(50) not null
	,id_dim_job_info varchar(50) not null
	,id_dim_job_post_status varchar(50) not null
	,id_dim_applicant varchar(50) not null
	,unique( job_id, id_dim_time, id_dim_company, id_dim_job_info, id_dim_job_post_status, id_dim_applicant)
	,constraint fk_time foreign key(id_dim_time) references dim_time(id_dim_time)
	,constraint fk_company foreign key(id_dim_company) references dim_company(id_dim_company)
	,constraint fk_job_info foreign key(id_dim_job_info) references dim_job_info(id_dim_job_info)
	,constraint fk_job_post_status foreign key(id_dim_job_post_status) references dim_job_post_status(id_dim_job_post_status)
	,constraint fk_applicant foreign key(id_dim_applicant) references dim_applicant(id_dim_applicant)
);
comment on column fact_job_applicant.id_fact_applicant is 'Primary key of this fact table. Surrogate key';
comment on column fact_job_applicant.job_id is 'Job Id. Degenerate Dimension';
comment on column fact_job_applicant.id_dim_time is 'Foreign key to time dimension';
comment on column fact_job_applicant.id_dim_company is 'Foreign key to company dimension';
comment on column fact_job_applicant.id_dim_job_info is 'Foreign key to job info dimension';
comment on column fact_job_applicant.id_dim_job_post_status is 'Foreign key to job post status dimension';
comment on column fact_job_applicant.id_dim_applicant is 'Foreign key to applicant dimension';

-- Replace the bucket for the correct one. Also replace the correct iam role
copy dim_time
from 's3://tecoloco***bucket/datawarehouse/dim_time'
iam_role 'arn:aws:iam::79171:role/service-role/AmazonRedshift-CommandsAccessRole-'
format as parquet;

copy dim_company
from 's3://tecoloco***bucket/datawarehouse/dim_company'
iam_role 'arn:aws:iam::79171:role/service-role/AmazonRedshift-CommandsAccessRole-'
format as parquet;

copy dim_job_info
from 's3://tecoloco***bucket/datawarehouse/dim_job_info'
iam_role 'arn:aws:iam::79171:role/service-role/AmazonRedshift-CommandsAccessRole-'
format as parquet;

copy dim_job_post_status
from 's3://tecoloco***bucket/datawarehouse/dim_job_post_status'
iam_role 'arn:aws:iam::79171:role/service-role/AmazonRedshift-CommandsAccessRole-'
format as parquet;

copy dim_applicant
from 's3://tecoloco***bucket/datawarehouse/dim_applicant'
iam_role 'arn:aws:iam::79171:role/service-role/AmazonRedshift-CommandsAccessRole-'
format as parquet;

copy fact_job_post
from 's3://tecoloco***bucket/datawarehouse/fact_job_post'
iam_role 'arn:aws:iam::79171:role/service-role/AmazonRedshift-CommandsAccessRole-'
format as parquet;

copy fact_job_applicant
from 's3://tecoloco***bucket/datawarehouse/fact_job_applicant'
iam_role 'arn:aws:iam::79171:role/service-role/AmazonRedshift-CommandsAccessRole-'
format as parquet;