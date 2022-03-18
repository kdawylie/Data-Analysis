--Unioning Task and Incident Tables so all data is represented

WITH cte AS (
    SELECT opened_at,
           opened_by,
           resolved_at AS closed_at
           assigned_to,
           assignment_group,
           number,
           location,
           category,
           CAST(
                DATE_DIFF('second' opened_at, resolved_at) AS VARCHAR
                ) AS time_worked_seconds
    FROM "data_lake"."table_with_data"
    WHERE TRIM(assigned_to) <> ''
            AND (LOWER(assignment_group) = 'desktop support'
            OR LOWER(assignment_group) = 'help desk')
            AND opened_at >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12' MONTH

    UNION ALL
    
    SELECT opened_at,
           opened_by,
           closed_at,
           assigned_to,
           assignment_group,
           number,
           location,
           '' AS category,
           CAST(
                DATE_DIFF('second', opened_at, closed_at) AS VARCHAR
                ) AS time_worked_seconds
    FROM "data_lake"."table_with_similar_data"
    WHERE (LOWER(assignment_group) = 'desktop support'
            OR LOWER(assignment_group) = 'help desk')
            AND opened_at >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12' MONTH
            
),

-- Using another cte to join employees to ensure proper population of assignment group employees

emps AS (
         SELECT name AS name_raw,
                CONCAT(
                       
                       -- These 4 employees go by different names than is represented in the directory, so we need to make sure
                       -- their data is populated by adjusting their names
                       -- Otherwise, name data is cleaned using split_part to make it more readable
                       
                       CASE
                            WHEN TRIM(name) = 'Nameson1. Name1 N1.' THEN 'Namey1'
                            WHEN TRIM(name) = 'Nameson2. Name2 N2.' THEN 'Namey2'
                            WHEN TRIM(name) = 'Nameson3. Name3 N3.' THEN 'Namey3'
                            ELSE TRIM(SPLIT_PART(SPLIT_PART(name, '. ', 2), ' ', 1))
                       END,
                       ' ',
                       CASE
                            WHEN TRIM(name) = 'Namer-Nameson4. Name4 N4.' THEN 'Nameson4' ELSE TRIM(SPLIT_PART(SPLIT_PART(name, '.', 1), ' ', 1))
                       END
                ) AS name,
                CONCAT(
                       TRIM(SPLIT_PART(SPLIT_PART(manager, '. ', 2), ' ', 1)),
                       ' ',
                       TRIM(SPLIT_PART(SPLIT_PART(manager, '.', ' ', 1))
                ) AS manager_name,
                email,
                "location" AS "agent_location",
                mgr_email AS manager_email,
         FROM "data_lake"."employee_table"
         WHERE extraction_date >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12' MONTH
            AND dep like '%End User%'
),
final AS (
            SELECT a.*,
                   b.email AS agent_email,
                   b.manager_name,
                   b.manager_email,
                   b.agent_location,
            CASE
                   WHEN number LIKE 'TASK%' THEN 'Task'
                   WHEN number LIKE 'INC%' THEN 'Incident'
            END AS ticket_type
            FROM cte a
                    JOIN
                 emps b ON a.assigned_to = b.name
)
SELECT DISTINCT *
FROM final
            
            
            
            
