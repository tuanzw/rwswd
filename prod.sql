select t2.clock_event_type, t1.payroll_id, t1.timezone, t2.datetime from
(select name, last_name payroll_id, 'Asia/Jarkata' timezone from acc_firstin_lastout where to_char(create_time,'yyyymmdd') = :wdate) t1 inner join 
(select name, last_name payroll_id, 'IN' clock_event_type, to_char(first_in_time ,'yyyy-mm-ddThh24:mi:ss') datetime from acc_firstin_lastout
where to_char(create_time,'yyyymmdd') = :wdate
union all 
select name, last_name payroll_id, 'OUT' clock_event_type, to_char(last_out_time ,'yyyy-mm-ddThh24:mi:ss') datetime from acc_firstin_lastout
where to_char(create_time,'yyyymmdd') = :wdate) t2
--on t1.payroll_id = t2.payroll_id
on t1.name = t2.name
where t2.payroll_id is not null
order by t1.name, t2.clock_event_type