SELECT
    (CURRENT_DATE - 1) AS ds
    , ID_worker_id
    , SUM(CASE WHEN is_present = 1 THEN 1 ELSE 0 END) AS RV_INT_num_unique_current_exps
    , SUM(CASE WHEN is_present = 0 THEN 1 ELSE 0 END) AS RV_INT_num_unique_past_exps
    , SUM(CASE WHEN is_staffing_agency = 1 THEN 1 ELSE 0 END) AS RV_INT_num_unique_staffing_agency_exps
    , SUM(CASE WHEN is_staffing_agency = 0 THEN 1 ELSE 0 END) AS RV_INT_num_unique_non_staffing_agency_exps
    , SUM(CASE WHEN is_present = 1 THEN exp_in_months ELSE 0 END) AS RV_INT_current_exp_months
    , SUM(CASE WHEN is_present = 0 THEN exp_in_months ELSE 0 END) AS RV_INT_past_exp_months
    , SUM(CASE WHEN is_staffing_agency = 1 THEN exp_in_months ELSE 0 END) AS RV_INT_staffing_agency_exp
    , SUM(CASE WHEN is_staffing_agency = 0 THEN exp_in_months ELSE 0 END) AS RV_INT_non_staffing_agency_exp
FROM (
    (
        SELECT 
            user_id AS ID_worker_id
            , is_present
            , business_id
            , is_staffing_agency
            , startmonth
            , startyear
            , endmonth
            , endyear
            , DATEDIFF(
                MONTH,
                TO_DATE(CONCAT(LPAD(COALESCE(startmonth, '01'), 2, '0'), startyear), 'MMYYYY')
                , TO_DATE(CONCAT(LPAD(COALESCE(endmonth, '01'), 2, '0'), endyear), 'MMYYYY')
            ) AS exp_in_months
            , date_created
            , date_modified
        FROM 
            {{ source("instawork-dw-backend", "backend_workexperience") }}
        WHERE
            is_deleted IS FALSE
            AND startyear IS NOT NULL
            AND endyear IS NOT NULL
            AND is_present IS FALSE
            AND exp_in_months > 0
            AND date_modified < CURRENT_DATE
            AND TO_DATE(CONCAT(LPAD(COALESCE(endmonth, '01'), 2, '0'), endyear), 'MMYYYY') < CURRENT_DATE
    ) UNION (
        SELECT 
            user_id AS ID_worker_id
            , is_present
            , business_id
            , is_staffing_agency
            , startmonth
            , startyear
            , endmonth
            , endyear
            , DATEDIFF(
                MONTH,
                TO_DATE(CONCAT(LPAD(COALESCE(startmonth,  '01'), 2, '0'), startyear), 'MMYYYY')
                , date_modified
            ) AS exp_in_months
            , date_created
            , date_modified
        FROM 
            {{ source("instawork-dw-backend", "backend_workexperience") }}
        WHERE
            is_deleted IS FALSE
            AND startyear IS NOT NULL
            AND is_present IS TRUE
            AND exp_in_months > 0
            AND date_modified < CURRENT_DATE
            AND TO_DATE(CONCAT(LPAD(COALESCE(endmonth, '01'), 2, '0'), endyear), 'MMYYYY') < CURRENT_DATE
    )
)
GROUP BY
    ID_worker_id

