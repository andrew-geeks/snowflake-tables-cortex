-- =============================================
-- CORTEX AGENT: ETL Pipeline with Custom Tool
-- =============================================

CREATE OR REPLACE PROCEDURE CORTEX.TESTING.RUN_ETL_PIPELINE()
RETURNS VARCHAR
LANGUAGE SQL
AS
BEGIN
    CREATE OR REPLACE TABLE CORTEX.TESTING.CUSTOMER_DIM LIKE CORTEX.PUBLIC.CUSTOMER_DIM;
    CREATE OR REPLACE TABLE CORTEX.TESTING.CUSTOMER_SALES LIKE CORTEX.PUBLIC.CUSTOMER_SALES;

    INSERT INTO CORTEX.TESTING.CUSTOMER_DIM (CUSTOMER_ID, CUSTOMER_NAME)
    SELECT $1::INT, $2::VARCHAR
    FROM @CORTEX.TESTING.CSV_STAGE/customer_dim.csv;

    INSERT INTO CORTEX.TESTING.CUSTOMER_SALES (CUSTOMER_ID, SALES)
    SELECT $1::INT, $2::INT
    FROM @CORTEX.TESTING.CSV_STAGE/customer_sales.csv;

    CREATE OR REPLACE TABLE CORTEX.TESTING.SALES_FACT AS
    SELECT
        d.CUSTOMER_ID,
        d.CUSTOMER_NAME,
        SUM(s.SALES) AS TOTAL_SALES
    FROM CORTEX.TESTING.CUSTOMER_DIM d
    INNER JOIN CORTEX.TESTING.CUSTOMER_SALES s
        ON d.CUSTOMER_ID = s.CUSTOMER_ID
    GROUP BY d.CUSTOMER_ID, d.CUSTOMER_NAME;

    CREATE OR REPLACE TABLE CORTEX.TESTING.SALES_FACT_CSV AS
    SELECT $1::INT AS CUSTOMER_ID, $2::VARCHAR AS CUSTOMER_NAME, $3::INT AS TOTAL_SALES
    FROM @CORTEX.TESTING.CSV_STAGE/sales_fact.csv;

    LET v_match INT := 0;
    LET v_mismatch INT := 0;
    LET v_proc_only INT := 0;
    LET v_csv_only INT := 0;

    SELECT COUNT(*) INTO :v_match
    FROM CORTEX.TESTING.SALES_FACT p
    INNER JOIN CORTEX.TESTING.SALES_FACT_CSV c
        ON p.CUSTOMER_ID = c.CUSTOMER_ID
        AND p.CUSTOMER_NAME = c.CUSTOMER_NAME
        AND p.TOTAL_SALES = c.TOTAL_SALES;

    SELECT COUNT(*) INTO :v_mismatch
    FROM CORTEX.TESTING.SALES_FACT p
    INNER JOIN CORTEX.TESTING.SALES_FACT_CSV c
        ON p.CUSTOMER_ID = c.CUSTOMER_ID
    WHERE p.TOTAL_SALES != c.TOTAL_SALES OR p.CUSTOMER_NAME != c.CUSTOMER_NAME;

    SELECT COUNT(*) INTO :v_proc_only
    FROM CORTEX.TESTING.SALES_FACT p
    LEFT JOIN CORTEX.TESTING.SALES_FACT_CSV c ON p.CUSTOMER_ID = c.CUSTOMER_ID
    WHERE c.CUSTOMER_ID IS NULL;

    SELECT COUNT(*) INTO :v_csv_only
    FROM CORTEX.TESTING.SALES_FACT_CSV c
    LEFT JOIN CORTEX.TESTING.SALES_FACT p ON c.CUSTOMER_ID = p.CUSTOMER_ID
    WHERE p.CUSTOMER_ID IS NULL;

    RETURN 'ETL Complete. Comparison: ' || :v_match || ' matched, ' || :v_mismatch || ' mismatched, ' || :v_proc_only || ' proc-only, ' || :v_csv_only || ' csv-only.';
END;

CALL CORTEX.TESTING.RUN_ETL_PIPELINE();

CREATE OR REPLACE AGENT CORTEX.TESTING.ETL_AGENT
  COMMENT = 'Agent that runs the ETL pipeline: copies table structures, loads CSVs, builds fact table, and compares results'
  PROFILE = '{"display_name": "ETL Pipeline Agent"}'
  FROM SPECIFICATION $$
  {
    "models": {
      "orchestration": "claude-4-sonnet"
    },
    "instructions": {
      "orchestration": "You are an ETL pipeline agent. When the user asks to run the pipeline, load data, compare results, or anything related to the ETL process, use the run_etl_pipeline tool. Report the comparison results clearly.",
      "response": "Be concise and report the ETL results in a structured format. Highlight any mismatches found during comparison."
    },
    "tools": [
      {
        "tool_spec": {
          "type": "generic",
          "name": "run_etl_pipeline",
          "description": "Runs the full ETL pipeline: copies CUSTOMER_DIM and CUSTOMER_SALES table structures from PUBLIC schema to TESTING schema, loads CSV data from csv_stage into them with proper type casting, creates SALES_FACT by joining and aggregating the data, then compares the procedure-generated SALES_FACT with the CSV sales_fact data. Returns match/mismatch counts.",
          "input_schema": {
            "type": "object",
            "properties": {},
            "required": []
          }
        }
      }
    ],
    "tool_resources": {
      "run_etl_pipeline": {
        "type": "procedure",
        "identifier": "CORTEX.TESTING.RUN_ETL_PIPELINE",
        "execution_environment": {
          "type": "warehouse",
          "name": "COMPUTE_WH"
        }
      }
    }
  }
  $$;