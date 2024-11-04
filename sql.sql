select t2.clock_event_type, t1.empid workday_worker_id, t1.timezone, t2.datetime from
(select empid, 'Asia/Jarkata' timezone from t_assignment) t1 inner join 
(select empid, 'IN' clock_event_type, to_char(createdat,'yyyymmdd hh24miss') datetime from t_assignment
union all 
select empid, 'OUT' clock_event_type, to_char(updatedat,'yyyymmdd hh24miss') datetime from t_assignment) t2
on t1.empid = t2.empid
where %s = %s
order by t1.empid, t2.clock_event_type