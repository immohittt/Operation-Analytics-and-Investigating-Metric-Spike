create database Project_three;
use project_three;
select * from job_data;

#____________________________________________CASE STUDY 1__________________________________________________#


#____________________________________________TASK A   (Jobs Reviewed Over Time)

SELECT 
    ds AS Date_, COUNT(job_id) / 24 AS Jobs_Reviewed_Per_Hour
FROM
    job_data
GROUP BY Date_;

#____________________________________________TASK B  (Language Share Analysis)

SELECT 
    COUNT(event) / (SELECT 
            COUNT(DISTINCT ds)
        FROM
            job_data) AS Avg_Throughput
FROM
    job_data;


#____________________________________________TASK C  ( Language Share Analysis )

SELECT 
    language,
    COUNT(*) AS frequncy,
    (COUNT(*) * 100) / (SELECT 
            COUNT(*)
        FROM
            job_data) AS percentage_share
FROM
    job_data
GROUP BY language; 

#____________________________________________TASK D  ( Duplicate Rows Detection )

select * from job_data;
SELECT 
    *
FROM
    job_data
GROUP BY ds , job_id , actor_id , event , language , time_spent , org
HAVING COUNT(*) > 1;


#____________________________________________ TO IMPORT LARGE DATA SET FOR CASE STUDY 2 __________________________________________________#

select * from events;
set sql_safe_updates = 0;
alter table events add column temp_created datetime;
update events set temp_created = str_to_date(occurred_at , '%d-%m-%Y %H:%i');
alter table events drop column occurred_at;
alter table events change column temp_created occurred_at datetime;

create table email_events(
user_id int,
occurred_at varchar(100),
action varchar(100),
user_type int);

show variables like 'secure_file_priv';
load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv"
into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


#____________________________________________  CASE STUDY 2  __________________________________________________#


#____________________________________________TASK A  (WEEKLY USER ENGAGEMENT)

select * from events;

select * from users;

SELECT 
    CONCAT(WEEKOFYEAR(activated_at),
            ' of ',
            YEAR(activated_at)) AS weeks,
    COUNT(user_id) AS users_active
FROM
    users
WHERE
    state = 'active'
GROUP BY weeks;

#select weekofyear(activated_at) as weeks, activated_at from users  having weeks= '1' ;
#select dayname("2013-12-30");

#____________________________________________TASK B  ( User Growth Analysis.)

SELECT 
    device,
    MONTH(occurred_at) AS `Month`,
    COUNT(DISTINCT (user_id)) AS Users_Signed_Up
FROM
    events
WHERE
    event_type = 'signup_flow'
GROUP BY device , `Month`;



#____________________________________________TASK C  (Weekly Engagement Per Device.)
 

SELECT 
    us.weeks,
    us.user_s AS users_signup,
    ur.user_r AS users_retain,
    (ur.user_r) / (us.user_s) * 100 AS percentage_of_user_retention
FROM
    (SELECT 
        WEEK(occurred_at) AS weeks,
            COUNT(DISTINCT user_id) AS user_s
    FROM
        events
    WHERE
        event_type = 'engagement'
    GROUP BY weeks) AS us
        JOIN
    (SELECT 
        WEEK(occurred_at) AS weeks,
            COUNT(DISTINCT user_id) AS user_r
    FROM
        events
    WHERE
        event_type = 'signup_flow'
    GROUP BY weeks) AS ur ON us.weeks = ur.weeks
GROUP BY weeks;




#________________________________________________TASK D  ( Weekly Engagement Per Device. )


SELECT 
	device,
    CONCAT(WEEKOFYEAR(occurred_at),
            ' of ',
            YEAR(occurred_at)) AS week_,
    COUNT(DISTINCT user_id) AS users_engaged
FROM
    events
GROUP BY week_ , device;
    

#__________________________________________________TASK E  (Email Engagement Analysis.)


SELECT 
    MONTH(occurred_at) AS Month,
    action,
    COUNT(user_id) AS `No._of_Users`
FROM
    email_events
GROUP BY month , action;
