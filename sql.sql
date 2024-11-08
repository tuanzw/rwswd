select t.empid as payroll_id, t.clock_event_type, t.timezone, t.datetime from (
    select empid, 'IN' clock_event_type, 'Asia/Jarkata' timezone, to_char(createdat,'yyyy-mm-ddThh24:mi:ss') datetime from t_assignment
    union all 
    select empid, 'OUT' clock_event_type, 'Asia/Jarkata' timezone, to_char(updatedat,'yyyy-mm-ddThh24:mi:ss') datetime from t_assignment
) t
where :wdate = :wdate
order by t.empid, t.clock_event_type