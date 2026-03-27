CREATE OR REPLACE PROCEDURE `analytics.prc_load_dashboard_costs`(
  VAR_PRJ_DATAPLAT_TRUSTED STRING,
  VAR_PRJ_DATAPLAT_REFINED STRING
) 
  BEGIN--Parameters for log control
  DECLARE VAR_PROCEDURE DEFAULT 'prc_load_ds_dashboards_costs';
  DECLARE VAR_DELTA_BEGIN DATE;
  DECLARE VAR_DELTA_END DATE;
  DECLARE VAR_TABLE STRING;
  DECLARE VAR_DATETIME_BEGIN TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
  

EXECUTE IMMEDIATE """
      CREATE TEMP TABLE DASHBOARD_DATA AS
      SELECT 
      p.date_month_year,
      p.dashboard_id,
      l.project_code,
      l.owner_email,
      p.last_project_code,
      l.dashboard_name,
      p.bytes_processed,
      p.user_count,
      p.access_count,
      p.execution_count,
      p.source_table_description
      FROM `""" || VAR_PROJECT_REFINED || """.analytics.dashboard_processing` p
      LEFT JOIN `""" || VAR_PROJECT_REFINED || """.analytics.dashboard_owners` l
      ON l.dashboard_code = p.dashboard_id
      WHERE active_dashboard_flag = TRUE
      AND p.date_month_year = DATE_TRUNC('""" ||VAR_START_DATE|| """', month)
      """;

EXECUTE IMMEDIATE """
      CREATE TEMP TABLE METRICS_DATA AS
      SELECT 
      j.date, 
      j.execution_description,
      j.billing_project_code,
      j.version_description,
      j.full_table_name,
      j.table_name,
      j.dataset_name,
      j.dataset_table_name,
      t.environment_description,
      t.parent_project_name,
      t.workbook_name,
      t.published_data_source_name,
      t.workbook_code,
      t.item_owner_name,
      l.dashboard_id,
      l.owner_email AS dashboard_owner_email,
      l.dashboard_name,
      SUM(j.bytes_processed) AS bytes_processed_total, 
      SUM(j.bytes_processed)/POW(1024,4) AS bytes_processed_tb, 
      MAX(t.user_count) AS tableau_user_count, 
      MAX(t.access_count) AS tableau_access_count,
      MAX(l.user_count) AS looker_user_count,
      MAX(l.access_count) AS looker_access_count
      FROM JOBS_HISTORY j  
      LEFT JOIN WORKBOOK_DATA t 
      ON j.date = t.date 
      AND UPPER(j.table_name) = UPPER(t.table_name) 
      AND UPPER(j.dataset_name) = UPPER(t.dataset_name) 
      AND UPPER(j.dataset_table_name) = UPPER(t.dataset_table_name)
      AND j.execution_description = 'Tableau'
      LEFT JOIN DASHBOARD_DATA l 
      ON  l.date_month_year = j.date 
      AND j.dashboard_id = l.dashboard_id 
      GROUP BY ALL 
      """;

EXECUTE IMMEDIATE """
      CREATE TEMP TABLE PROCESSED_TABLE AS
      SELECT 
      date,
      execution_description,
      billing_project_code,
      version_description,
      item_owner_name,
      full_table_name,
      environment_description,
      workbook_name,
      workbook_code,
      dashboard_id,
      dashboard_owner_email,
      dashboard_name,
      bytes_processed_tb,
      tableau_access_count,
      tableau_user_count,
      looker_user_count,
      looker_access_count,
      ROW_NUMBER() OVER (
        PARTITION BY date, execution_description, billing_project_code, full_table_name
        ORDER BY workbook_name
      ) AS row_num
      FROM METRICS_DATA
      """;

EXECUTE IMMEDIATE """
      CREATE TEMP TABLE CLEANED_PROCESSED_TABLE AS
      SELECT 
      date,
      execution_description,
      billing_project_code,
      version_description,
      full_table_name,
      environment_description,
      workbook_name,
      workbook_code,
      dashboard_id,
      item_owner_name,
      dashboard_owner_email,
      dashboard_name,
      bytes_processed_tb,
      tableau_user_count,
      tableau_access_count,
      looker_user_count,
      looker_access_count,
      SUM(CASE WHEN row_num = 1 THEN bytes_processed_tb ELSE 0 END) AS bytes_processed_tableau_total
    FROM 
      PROCESSED_TABLE 
    GROUP BY ALL
      """;

EXECUTE IMMEDIATE """
        CREATE TEMP TABLE FINAL_TABLE AS
      SELECT 
        date AS date_month_year,
        execution_description AS tool_description,
        billing_project_code,
        version_description,
        environment_description,
        full_table_name,
        workbook_code AS dashboard_code,
        workbook_name AS dashboard_name,
        item_owner_name AS owner_email,
        bytes_processed_tb AS total_bytes_processed,
        SUM(MAX(tableau_access_count)) OVER (PARTITION BY date, execution_description, billing_project_code, full_table_name) AS total_access_table,
        MAX(MAX(tableau_access_count)) OVER (PARTITION BY date, execution_description, billing_project_code, full_table_name, workbook_code, workbook_name) AS access_count,
        CASE WHEN workbook_code IS NULL THEN bytes_processed_tb ELSE
            bytes_processed_tb * (
                    MAX(MAX(tableau_access_count)) OVER (PARTITION BY date, execution_description, billing_project_code, full_table_name, workbook_code, workbook_name) /
                    SUM(MAX(tableau_access_count)) OVER (PARTITION BY date, execution_description, billing_project_code, full_table_name)
                     ) 
        END AS bytes_processed,
        MAX(MAX(tableau_user_count)) OVER (PARTITION BY date, execution_description, workbook_name) AS users_per_month
        FROM CLEANED_PROCESSED_TABLE
        WHERE execution_description ='Tableau'
        GROUP BY ALL
        """;

EXECUTE IMMEDIATE """
        CREATE TEMP TABLE FINAL_AGGREGATE AS
        SELECT 
        date_month_year,
        tool_description,
        billing_project_code,
        version_description,
        environment_description,
        STRING_AGG(full_table_name, ', ') AS full_table_name,
        dashboard_code,
        CASE WHEN dashboard_name IS NULL THEN 'Not Identified' ELSE dashboard_name END AS dashboard_name,
        owner_email,
        SUM(bytes_processed) AS bytes_processed_total,
        access_count,
        users_per_month,
        SUM(bytes_processed) * 30 AS estimated_processing_cost_brl
        FROM FINAL_TABLE
        GROUP BY ALL

        UNION ALL
    
        SELECT 
        date AS date_month_year,
        execution_description AS tool_description,
        billing_project_code,
        version_description,
        'Looker Studio' AS environment_description,
        STRING_AGG(full_table_name, ', ') AS full_table_name,
        dashboard_id AS dashboard_code,
        CASE WHEN dashboard_name IS NULL THEN 'Not Identified' ELSE dashboard_name END AS dashboard_name,
        dashboard_owner_email AS owner_email,
        SUM(bytes_processed) AS bytes_processed_total, 
        MAX(MAX(looker_access_count)) OVER (PARTITION BY date, execution_description, dashboard_id) AS access_count,
        MAX(MAX(looker_user_count)) OVER (PARTITION BY date, execution_description, dashboard_id) AS users_per_month,
        SUM(bytes_processed) * 30 AS estimated_processing_cost_brl
        FROM CLEANED_PROCESSED_TABLE
        WHERE execution_description ='Looker Studio'
        GROUP BY ALL
        """;
 -- =============================================
  -- TRUNC TABLE
  -- =============================================
  EXECUTE IMMEDIATE """
          DELETE FROM  `""" || VAR_PRJ_DATAPLAT_REFINED || """.""" || VAR_TABLE || """`
          WHERE DATE = DATE_TRUNC('""" ||VAR_DELTA_BEGIN|| """', month)
        """;
  
  -- =============================================
  -- INSERT NEW RECORDS
  -- =============================================
  EXECUTE IMMEDIATE """
          INSERT INTO `""" || VAR_PRJ_DATAPLAT_REFINED || """.""" || VAR_TABLE || """`
          SELECT 
            *
          FROM 
            TABLE_FINAL
        """;
  
  
  --record logs 
  CALL sp.prc_log_exec(
    VAR_TABLE,
    VAR_DATETIME_BEGIN,
    @@ row_count,
    VAR_PROCEDURE,
    @@ error.message,
    VAR_PRJ_DATAPLAT_REFINED
  );
  
  
  EXCEPTION
  WHEN ERROR THEN CALL sp.prc_log_exec(
    VAR_TABLE,
    VAR_DATETIME_BEGIN,
    @@ row_count,
    VAR_PROCEDURE,
    @@ error.message,
    VAR_PRJ_DATAPLAT_REFINED
  );
  
  
  RAISE USING MESSAGE = @@ error.message;
  
  
  END;
  

END;


----DEV
CALL `analytics.prc_load_dashboard_costs`('trusted-data-project','dataviz-project')