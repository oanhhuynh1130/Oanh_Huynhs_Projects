# -*- coding: utf-8 -*-
"""Duplicated Job Check.ipynb



# 1 . Import lib
"""
#Export to Google Sheets / Part 1 Auth
from google.colab import auth
auth.authenticate_user()

import gspread
from google.auth import default
creds, _ = default()

gc = gspread.authorize(creds)

import pandas as pd
import psycopg2
import seaborn as sns
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np

"""# 2 . Start Cursor"""

conn = psycopg2.connect(host="",
                        port = "", database="", user="",
                        password="")

cur = conn.cursor()

"""# 3 . Get data"""

cur.execute("""
  SELECT
    "glints"."jobs"."id" AS "job_id",
    ("glints"."jobs"."description_raw") :: text AS "job_description",
    "glints"."jobs"."title" AS "job_title",
    "glints"."jobs"."type" AS "job_type",
    "glints"."jobs"."created_at" AS "job_created_at",
    "glints"."jobs"."employer_service_rep_id" AS "employer_service_rep_id",
    "glints"."jobs"."fraud_report_flag" AS "fraud_report_flag",
    "Hierarchical Job Categories"."default_name" AS "category_l3",
    "Hierarchical Job Categories_2"."default_name" AS "category_l2",
    "Hierarchical Job Categories_3"."default_name" AS "category_l1",
    "Companies"."id" AS "company_id",
    "Companies"."name" AS "company_name",
    "Companies"."created_at" AS "company_created_at",
    "Companies"."legal_registration_number" AS "company_legal_registration_number",
    "Companies"."legal_document" AS "legal_document",
    "Companies"."status" AS "company_status",
    "Companies"."acquisition_referral_code" AS "company_acquisition_referral_code",
    "Locations"."default_name" AS "job_district",
    "Locations_2"."default_name" AS "job_city",
    "Locations_3"."default_name" AS "company_city",
    i.name AS "company_industry"
  FROM
    "glints"."jobs"

  LEFT JOIN "glints"."hierarchical_job_categories" AS "Hierarchical Job Categories" ON "glints"."jobs"."hierarchical_job_category_id" = "Hierarchical Job Categories"."id"
    LEFT JOIN "glints"."job_categories" AS "Job Categories" ON "glints"."jobs"."job_category_id" = "Job Categories"."id"
    LEFT JOIN "glints"."hierarchical_job_categories_mptt" AS "Hierarchical Job Categories Mptt" ON "Hierarchical Job Categories"."id" = "Hierarchical Job Categories Mptt"."id"
    LEFT JOIN "glints"."hierarchical_job_categories" AS "Hierarchical Job Categories_2" ON "Hierarchical Job Categories Mptt"."parent_id" = "Hierarchical Job Categories_2"."id"
    LEFT JOIN "glints"."hierarchical_job_categories_mptt" AS "Hierarchical Job Categories Mptt_2" ON "Hierarchical Job Categories_2"."id" = "Hierarchical Job Categories Mptt_2"."id"
    LEFT JOIN "glints"."hierarchical_job_categories" AS "Hierarchical Job Categories_3" ON "Hierarchical Job Categories Mptt_2"."parent_id" = "Hierarchical Job Categories_3"."id"
    LEFT JOIN "glints"."companies" AS "Companies" ON "glints"."jobs"."company_id" = "Companies"."id"
    LEFT JOIN "glints"."locations" AS "Locations" ON "glints"."jobs"."district_id" = "Locations"."id"
    LEFT JOIN "glints"."location_mptt" AS "Location Mptt" ON "Locations"."id" = "Location Mptt"."id"
    LEFT JOIN "glints"."locations" AS "Locations_2" ON "Location Mptt"."parent_id" = "Locations_2"."id"
    LEFT JOIN "glints"."locations" AS "Locations_3" ON "Companies"."location_id" = "Locations_3"."id"
    LEFT JOIN glints.industries AS i ON "Companies"."industry_id" = i.id
  WHERE
    ("glints"."jobs"."country_code" = 'VN')
    AND ("Companies"."status" = 'VERIFIED')
    AND ("glints"."jobs"."status" = 'OPEN')
  ORDER BY
    "Companies"."name",
    "Companies"."id",
    "Hierarchical Job Categories"."default_name",
    "glints"."jobs"."type",
    "Locations_2"."default_name",
    "Locations"."default_name"
""")


all_opening_job = pd.DataFrame(cur.fetchall())
all_opening_job.columns = [i[0] for i in cur.description]

cur.execute("""
WITH Similarity AS (
    SELECT
        j1.id AS job_id_1,
        j1.title AS job_title_1,
        j2.id AS job_id_2,
        j2.title AS job_title_2,
        (SELECT string_agg(blocks->>'text', ' ') FROM jsonb_array_elements(j1.description_raw->'blocks') AS blocks) :: text AS job_description_1,
        (SELECT string_agg(blocks->>'text', ' ') FROM jsonb_array_elements(j2.description_raw->'blocks') AS blocks) :: text AS job_description_2,
        similarity((SELECT string_agg(blocks->>'text', ' ') FROM jsonb_array_elements(j1.description_raw->'blocks') AS blocks), (SELECT string_agg(blocks->>'text', ' ') FROM jsonb_array_elements(j2.description_raw->'blocks') AS blocks)) AS sim
    FROM
        glints.jobs j1
    CROSS JOIN
        glints.jobs j2
    LEFT JOIN
        glints.companies c1 ON j1.company_id = c1.id
    LEFT JOIN
        glints.companies c2 ON j2.company_id = c2.id
    WHERE
        j1.id <> j2.id
        AND j1.created_at < j2.created_at
        AND j1.status = 'OPEN' AND j2.status = 'OPEN'
        AND c1.name = c2.name
        AND j1.district_id = j2.district_id
        AND j1.country_code = 'VN' AND j2.country_code = 'VN'
        AND c1.status = 'VERIFIED' AND c2.status = 'VERIFIED'

)
-- Uncomment this for the total number instead of breakdown
-- SELECT COUNT(*)
-- FROM Similarity
-- WHERE sim > 0.9;
SELECT
    job_id_1,
    job_title_1,
    ROW_NUMBER() OVER (partition by job_id_1) AS row,
    job_id_2,
    job_title_2
FROM Similarity
WHERE sim > 0.8
ORDER BY 1;


""")

similarity_more_than_80 = pd.DataFrame(cur.fetchall())
similarity_more_than_80.columns = [i[0] for i in cur.description]

cur.execute("""
WITH

job_hierarchical AS (
    SELECT
      a.id,
      a.default_name AS l3,
      c.default_name AS l2,
      e.default_name AS l1
    FROM glints.hierarchical_job_categories a
    LEFT JOIN glints.hierarchical_job_categories_mptt b ON a.id = b.id
    LEFT JOIN glints.hierarchical_job_categories c ON c.id = b.parent_id
    LEFT JOIN glints.hierarchical_job_categories_mptt d ON c.id = d.id
    LEFT JOIN glints.hierarchical_job_categories e ON e.id = d.parent_id
    WHERE b.level = 3
  ),

location_hierarchical AS (
    SELECT
      a.id,
      a.default_name AS l3,   -- District
      c.default_name AS l2,   -- City
      e.default_name AS l1    -- Country
    FROM glints.locations a
    LEFT JOIN glints.location_mptt b ON a.id = b.id
    LEFT JOIN glints.locations c ON c.id = b.parent_id
    LEFT JOIN glints.location_mptt d ON c.id = d.id
    LEFT JOIN glints.locations e ON e.id = d.parent_id
    LEFT JOIN glints.location_mptt f ON e.id = f.id
    LEFT JOIN glints.locations g ON g.id = f.parent_id
    WHERE b.level = 3
),

company_ids_from_sales_team_reactivation AS (
    SELECT
        hal.user_id AS pic,
        pic.email AS pic_email,
        hrc.company_id,
        hal.created_at AS reactivated_at
    FROM
       glints.houston_action_logs AS hal
    JOIN
        glints.houston_reactivate_company_action_logs AS hrc
    ON
        hal.id = hrc.houston_action_log_id
    LEFT JOIN
        glints.users AS pic
    ON hal.user_id = pic.id
    LEFT JOIN
        glints.companies  AS c
    ON
        hrc.company_id = c.id
    WHERE
        c.status = 'VERIFIED'
        AND c.country_code = 'VN'
        AND hal.action = 'REACTIVATE_COMPANY'

),

job_after_reactivated AS (
    SELECT
        j.id as job_id,
        j.created_at,
        cstr.company_id,
        cstr.pic_email
    FROM
        company_ids_from_sales_team_reactivation AS cstr
    LEFT JOIN
        glints.jobs AS j
    ON
        cstr.company_id = j.company_id
    WHERE
        j.created_at <= cstr.reactivated_at + INTERVAL '62 DAYS'
        AND j.created_at >= cstr.reactivated_at
),

company_ids_from_sales_team_with_no_job_after_reactivated AS (
    SELECT
        cstr.company_id
    FROM
        company_ids_from_sales_team_reactivation AS cstr
    LEFT JOIN
        job_after_reactivated AS jar
    ON
        cstr.company_id = jar.company_id
    WHERE
        jar.job_id IS NULL

),

company_ids_from_sales_team_referral AS (
    SELECT
        DISTINCT c.id AS company_id,
        CASE
            WHEN pic.email IS NULL THEN
                CASE
                    WHEN c.acquisition_referral_code LIKE '%GLVN-093%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-094%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-096%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-115%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-123%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-397%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-394%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-398%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-399%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-400%' THEN '@glints.com'
                    WHEN c.acquisition_referral_code LIKE '%GLVN-402%' THEN '@glints.com'
                    ELSE 'Others'
                END
            WHEN pic.email IS NOT NULL THEN pic.email
        END AS pic_email
    FROM
        glints.companies AS c
    LEFT JOIN
        glints.users AS pic
    ON
        c.employer_service_rep_id = pic.id
    WHERE
        (c.acquisition_referral_code LIKE 'GLVN%'
        OR c.employer_service_rep_id IN (
          '97bf4bc4-9ddc-4d89-b99f', -- Old - @glints.com
          '59615828-297c-4c47-a3eb', -- Old - @glints.com
          'e98481aa-2dd4-428e-aa08', -- Old - @glints.com
          '97d21ad3-55f5-430e-a85a', -- Old - @glints.com
          '660ebc20-2c1b-45ad-9d31', -- Old - @glints.com
          'b3981bbd-0875-4518-a6e8', -- Old - @glints.com
          '0fab2727-b28e-40ed-898b', -- Old - @glints.com
          '13dc3512-0760-48e2-85f3', -- Old - @glints.vn
          'ee9ba917-0f04-4bdc-b066', -- @glints.com
          '36b6cdec-b2f5-4d26-81d2', -- @glints.com
          '84f3acc8-b2b7-4061-99e4', -- @glints.com
          '4c347584-87a7-47b0-a0a5', -- @glints.com
          '6384dbea-7243-451a-ae5b', -- @glints.com
          'a26d4498-95b2-4c10-9c15' -- @glints.com
        ))
        AND c.status = 'VERIFIED'
        AND c.country_code = 'VN'
),

-- Companies from sales team employer account created - employer_service_rep_id in Users table
company_ids_from_sales_team_employer_account_created_rn AS (
    SELECT
        DISTINCT c.id AS company_id,
        pic.email AS pic_email,
        row_number() OVER( partition by c.id  ORDER BY u.created_at) AS rn
    FROM
        glints.users AS u
    JOIN
        glints.user_companies AS uc
    ON
        uc.user_id = u.id
    JOIN
        glints.companies AS c
    ON
        uc.company_id = c.id
    LEFT JOIN
        glints.users AS pic
    ON
        u.employer_service_rep_id = pic.id
    WHERE
        u.employer_service_rep_id IS NOT NULL
        AND c.status = 'VERIFIED'
        AND c.country_code = 'VN'
),

company_ids_from_sales_team_employer_account_created AS (
    SELECT *
    FROM company_ids_from_sales_team_employer_account_created_rn
    WHERE rn = 1 -- Chose the first pic in employer account created
),

-- Union distinct 2 list above to remove duplicated companies - include pic email
company_ids_pic_from_sales_team AS (
    SELECT *
    FROM (
        SELECT
            company_id,
            pic_email,
            acquired_type,
            row_number() OVER (partition by company_id ORDER BY acquired_type) AS rn
        FROM
            (SELECT
                company_id,
                pic_email,
                'company_profile' as acquired_type
            FROM
                company_ids_from_sales_team_referral
            UNION DISTINCT
            SELECT
                company_id,
                pic_email,
                'employer_account' as acquired_type
            FROM
                company_ids_from_sales_team_employer_account_created) AS a) AS b
    WHERE rn = 1 -- Chose company_profile pic if there's others pic acquired_type
),

companies_from_sale_team AS (
  SELECT
      CASE
        WHEN c.reviewed_at IS NOT NULL THEN c.reviewed_at
        ELSE c.created_at
      END AS verified_date,
      c.created_at,
      c.status,
      c.country_code,
      c.name,
      c.id AS company_id,
      c.reviewed_at,
      c.industry_id,
      cpst.pic_email,
      c.size
    FROM company_ids_pic_from_sales_team AS cpst
    LEFT JOIN glints.companies AS c
        ON cpst.company_id = c.id
    LEFT JOIN company_ids_from_sales_team_with_no_job_after_reactivated AS cnjar  -- Exclude reactivated company with no job after that
    ON cpst.company_id = cnjar.company_id
    WHERE
    cnjar.company_id IS NULL
),

jobs AS (
  SELECT
      j.id,
      j.title,
      CASE
          -- If jobs created before company get verified => Use the verified_date as job created date
            WHEN j.created_at < c.reviewed_at
                AND c.reviewed_at IS NOT NULL
                AND j.created_at < c.reviewed_at
                AND j.status = 'OPEN'
                AND ((c.employer_service_rep_end_date IS NULL) OR (c.employer_service_rep_end_date < j.created_at))
                THEN c.reviewed_at
            WHEN j.created_at < c.reviewed_at
                AND c.reviewed_at IS NOT NULL
                AND j.created_at < c.reviewed_at
                AND j.status = 'CLOSED'
                AND j.closed_at < c.reviewed_at
                THEN NULL
            ELSE j.created_at
      END AS created_at,
      j.created_at as job_original_created_at,
      j.employer_service_rep_id,
      j.country_code,
      c.id AS company_id,
      j.hierarchical_job_category_id,
      j.district_id,
      j.min_years_of_experience,
      j.closed_at AS job_closed_at
  FROM
      glints.jobs AS j
  LEFT JOIN
      glints.companies AS c
  ON
      c.id = j.company_id
  WHERE
      c.country_code = 'VN'
),

shortlisted_jobs AS (
    SELECT
        a.job_id,
        a.id AS application_id,
        (SELECT MIN(asl.created_at)
         FROM glints.application_status_logs as asl
         WHERE asl.application_id = a.id
           AND asl.created_at >= '2024-03-01 00:00:00'
           AND asl.status not in ('NEW','REJECTED')
        ) AS shortlisted_at
    FROM
        glints.applications as a
    INNER JOIN
        glints.users as u ON u.id = a.applicant_id
    WHERE
        a.created_at >= '2024-03-01 00:00:00'
        AND a.status <> 'NEW'
        AND u.country_code = 'VN'
),

final AS (
SELECT
    DATE_TRUNC('week', j.created_at) AS week_start_date,
    CASE
        WHEN cstr.company_id IS NOT NULL
            AND j.created_at <= reactivated_at + INTERVAL '62 DAYS'
            AND j.created_at > reactivated_at + INTERVAL '31 DAYS'
            AND j.employer_service_rep_id IS NULL
            THEN 'reactivated_in_2m'
        WHEN cstr.company_id IS NOT NULL
            AND j.created_at <= reactivated_at + INTERVAL '31 DAYS'
            AND j.created_at >= reactivated_at
            THEN 'reactivated_in_1m'
         WHEN cstr.company_id IS NOT NULL
            AND j.created_at < reactivated_at
                THEN NULL
        WHEN cstr.company_id IS NULL
            AND j.created_at <= cst.verified_date + INTERVAL '31 DAYS'
                THEN 'jobs_under_sale_team_1st_m'
        WHEN cstr.company_id IS NULL
            AND j.created_at > cst.verified_date + INTERVAL '31 DAYS'
            AND j.created_at <= cst.verified_date + INTERVAL '62 DAYS'
            AND j.employer_service_rep_id IS NULL
                THEN 'jobs_under_sale_team_2nd_m'
    END AS type,
    cst.pic_email,
    j.id AS job_id,
    j.title,
    jh.l1 AS job_category,
    lh.l2 AS job_location,
    cst.name AS company_name,
    COUNT(sj.application_id)  AS total_application,
    COUNT(sj.application_id) FILTER (WHERE sj.shortlisted_at <= j.created_at + INTERVAL '14 DAYS') AS total_14d_shortlisted_application,
    cst.company_id,
    cst.size AS company_size,
    i.name AS industry,
    cst.status,
    j.created_at AS job_created_at,
    j.min_years_of_experience,
    cst.created_at AS company_created_at,
    cst.reviewed_at AS company_reviewed_at,
    reactivated_at AS company_reactivated_at,
    job_closed_at
FROM
    jobs AS j
JOIN
    companies_from_sale_team AS cst
ON
    cst.company_id = j.company_id
LEFT JOIN
    company_ids_from_sales_team_reactivation AS cstr
ON
    j.company_id = cstr.company_id
LEFT JOIN
    shortlisted_jobs AS sj
ON
    j.id = sj.job_id
LEFT JOIN
    job_hierarchical AS jh
ON
    j.hierarchical_job_category_id = jh.id
LEFT JOIN
    location_hierarchical AS lh
ON
    j.district_id = lh.id
LEFT JOIN
    glints.industries AS i
ON
    cst.industry_id = i.id
WHERE
    cst.status = 'VERIFIED'
GROUP BY 1,2,3,4,5,6,7,8,11,12,13,14,15,16,17,18,19,20
ORDER BY
    1 DESC
)

SELECT *
FROM final
WHERE
    type IS NOT NULL
ORDER BY week_start_date DESC, pic_email, company_name

""")

outbound_jobs = pd.DataFrame(cur.fetchall())
outbound_jobs.columns = [i[0] for i in cur.description]

# outbound_jobs = pd.read_csv('/content/job_detail.csv')

all_opening_job.info()

similarity_more_than_80.info()

outbound_jobs.info()

all_opening_job.head()

conn.close()

"""# 3 . Check duplicated

## Reset data
"""

check_all_opening_job = all_opening_job

# Select columns 'company_name', 'company_id', and 'company_city'
company_df = check_all_opening_job[['company_name', 'company_id', 'company_city', 'company_industry','company_created_at']]

# Remove duplicate rows
company_df = company_df.drop_duplicates()

# Print the company DataFrame
company_df

"""## 3.1 Exactly Duplicated Jobs


*   Exact job_title
*   Exact job_description
*   Exact job_district
*   Exact company_name
*   Exact job_type


"""

# Count occurrences of combinations of values in columns job_title, job_description, company_name, and job_district
check_all_opening_job['exact_combination_count'] = check_all_opening_job.groupby(['job_title', 'job_description', 'company_name', 'job_district', 'job_type'])['job_title'].transform('count')

# Assign sequential numbers within each duplicate group
check_all_opening_job['exactly_duplicated_check'] = check_all_opening_job.groupby(['job_title', 'job_description', 'company_name', 'job_district', 'job_type']).cumcount() + 1

# For rows with a single occurrence, set exactly_duplicated_check to 0
check_all_opening_job.loc[check_all_opening_job['exact_combination_count'] == 1, 'exactly_duplicated_check'] = 0

# Create the new column 'exactly_duplication_mark' and fill it with 'x' where 'exactly_duplicated_check' is not 0
check_all_opening_job['exactly_duplication_mark'] = np.where(check_all_opening_job['exactly_duplicated_check'] != 0, 'x', '')

check_all_opening_job.head()

"""## 3.2 Partial Duplicated - Same District Jobs

*   Exact category_l3
*   Exact job_district
*   Exact company_name
*   Exact job_type
"""

# Count occurrences of combinations of values in columns category_l3, company_name, and job_district
check_all_opening_job['partial_sd_combination_count'] = check_all_opening_job.groupby(['category_l3', 'company_name', 'job_district', 'job_type'])['category_l3'].transform('count')

# Assign sequential numbers within each duplicate group
check_all_opening_job['partial_duplicated_check_same_district'] = 0
check_all_opening_job.loc[(check_all_opening_job['partial_sd_combination_count'] > 1) & (check_all_opening_job['exactly_duplicated_check'].isin([0])), 'partial_duplicated_check_same_district'] = check_all_opening_job[(check_all_opening_job['partial_sd_combination_count'] > 1) & (check_all_opening_job['exactly_duplicated_check'].isin([0]))].groupby(['category_l3', 'company_name', 'job_district', 'job_type']).cumcount() + 1

# For rows with a single occurrence, set partial_duplicated_check_same_district to 0

# Create the new column 'partial_duplication_same_location_count_mark' and fill it with 'x' where 'partial_duplicated_check_same_district' is not 0
check_all_opening_job['partial_duplication_same_location_count_mark'] = np.where(check_all_opening_job['partial_duplicated_check_same_district'] != 0, 'x', '')

check_all_opening_job.head()

# Initialize the new column with empty strings
check_all_opening_job['partial_duplicated_check_same_district_sim80_mark'] = ''

# Iterate over each group formed by the combination of category_l3, company_name, job_district, job_type
for group_keys, group_df in check_all_opening_job.groupby(['category_l3', 'company_name', 'job_district', 'job_type']):

    # Get the job with partial_duplicated_check_same_district == 1
    job_id_1_row = group_df[group_df['partial_duplicated_check_same_district'] == 1]

    # If there is no such job, continue to the next group
    if job_id_1_row.empty:
        continue

    # Get the job_id_1 (there should be only one, take the first if there are multiple)
    job_id_1 = job_id_1_row.iloc[0]['job_id']  # Assuming there is a column named 'job_id'

    # Filter out the jobs with partial_duplicated_check_same_district != 1
    other_jobs = group_df[group_df['partial_duplicated_check_same_district'] != 1]

    # Check similarity between job_id_1 and other jobs
    for index, job in other_jobs.iterrows():
        job_id_2 = job['job_id']

        # Check similarity_more_than_80 DataFrame for a match
        similarity_match = similarity_more_than_80[
            ((similarity_more_than_80['job_id_1'] == job_id_1) & (similarity_more_than_80['job_id_2'] == job_id_2)) |
            ((similarity_more_than_80['job_id_1'] == job_id_2) & (similarity_more_than_80['job_id_2'] == job_id_1))
        ]

        # If a match is found, mark 'x' in the new column
        if not similarity_match.empty:
            check_all_opening_job.loc[check_all_opening_job['job_id'] == job_id_1, 'partial_duplicated_check_same_district_sim80_mark'] = 'x'
            check_all_opening_job.loc[check_all_opening_job['job_id'] == job_id_2, 'partial_duplicated_check_same_district_sim80_mark'] = 'x'

#  Since there's many duplicated intented change, Mark 'x' for jobs with partial_sd_combination_count > 5
check_all_opening_job.loc[check_all_opening_job['partial_sd_combination_count'] > 4, 'partial_duplicated_check_same_district_sim80_mark'] = 'x'

# Assign sequential numbers within each duplicate group
check_all_opening_job['partial_duplicated_check_same_district_sim80'] = 0
check_all_opening_job.loc[(check_all_opening_job['partial_duplicated_check_same_district_sim80_mark'] == 'x'), 'partial_duplicated_check_same_district_sim80'] = check_all_opening_job[(check_all_opening_job['partial_duplicated_check_same_district_sim80_mark'] == 'x')].groupby(['category_l3', 'company_name', 'job_district', 'job_type']).cumcount() + 1

"""## 3.3 Partial Duplicated - Different District Jobs

*   Exact category_l3
*   Different job_district
*   Exact job_city
*   Exact company_name
*   Exact job_type
"""

# Count occurrences of combinations of values in columns category_l3, company_name, and job_city
check_all_opening_job['combination_count'] = check_all_opening_job.groupby(['category_l3', 'company_name', 'job_city', 'job_type'])['category_l3'].transform('count')

# Check for different job_district within each group
check_all_opening_job['partial_dd_combination_count'] = check_all_opening_job.groupby(['category_l3', 'company_name', 'job_city', 'job_type'])['job_district'].transform('nunique')

# Assign sequential numbers within each duplicate group where job_districts are different, partial_duplicated_check_same_district is 0, and exactly_duplicated_check is 0
check_all_opening_job['partial_duplicated_check_different_district'] = 0
condition = (check_all_opening_job['partial_dd_combination_count'] > 1) &  (check_all_opening_job['partial_duplication_same_location_count_mark'] == '')  & (check_all_opening_job['exactly_duplicated_check'].isin([0]))
check_all_opening_job.loc[condition, 'partial_duplicated_check_different_district'] = check_all_opening_job[condition].groupby(['category_l3', 'company_name', 'job_city', 'job_type']).cumcount() + 1

# Create the new column 'partial_duplication_diff_location_count_mark' and fill it with 'x' where 'partial_duplicated_check_different_district' is not 0
check_all_opening_job['partial_duplication_diff_location_count_mark'] = np.where(check_all_opening_job['partial_duplicated_check_different_district'] != 0, 'x', '')

# Initialize the new column with empty strings
check_all_opening_job['partial_duplicated_check_diff_district_sim80_mark'] = ''

# Iterate over each group formed by the combination of category_l3, company_name, job_district, job_type
for group_keys, group_df in check_all_opening_job.groupby(['category_l3', 'company_name', 'job_city', 'job_type']):

    # Get the job with partial_duplicated_check_same_district == 1
    job_id_1_row = group_df[group_df['partial_duplicated_check_different_district'] == 1]

    # If there is no such job, continue to the next group
    if job_id_1_row.empty:
        continue

    # Get the job_id_1 (there should be only one, take the first if there are multiple)
    job_id_1 = job_id_1_row.iloc[0]['job_id']  # Assuming there is a column named 'job_id'

    # Filter out the jobs with partial_duplicated_check_same_district != 1
    other_jobs = group_df[group_df['partial_duplicated_check_different_district'] != 1]

    # Check similarity between job_id_1 and other jobs
    for index, job in other_jobs.iterrows():
        job_id_2 = job['job_id']

        # Check similarity_more_than_80 DataFrame for a match
        similarity_match = similarity_more_than_80[
            ((similarity_more_than_80['job_id_1'] == job_id_1) & (similarity_more_than_80['job_id_2'] == job_id_2)) |
            ((similarity_more_than_80['job_id_1'] == job_id_2) & (similarity_more_than_80['job_id_2'] == job_id_1))
        ]

        # If a match is found, mark 'x' in the new column
        if not similarity_match.empty:
            check_all_opening_job.loc[check_all_opening_job['job_id'] == job_id_1, 'partial_duplicated_check_diff_district_sim80_mark'] = 'x'
            check_all_opening_job.loc[check_all_opening_job['job_id'] == job_id_2, 'partial_duplicated_check_diff_district_sim80_mark'] = 'x'

#  Since there's many duplicated intented change, Mark 'x' for jobs with partial_sd_combination_count > 5
check_all_opening_job.loc[(check_all_opening_job['partial_dd_combination_count'] > 5) & (check_all_opening_job['partial_duplicated_check_same_district_sim80_mark'] == ''), 'partial_duplicated_check_diff_district_sim80_mark'] = 'x'

# Assign sequential numbers within each duplicate group
check_all_opening_job['partial_duplicated_check_diff_district_sim80'] = 0
check_all_opening_job.loc[(check_all_opening_job['partial_duplicated_check_diff_district_sim80_mark'] == 'x'), 'partial_duplicated_check_diff_district_sim80'] = check_all_opening_job[(check_all_opening_job['partial_duplicated_check_diff_district_sim80_mark'] == 'x')].groupby(['category_l3', 'company_name', 'job_city', 'job_type']).cumcount() + 1

check_all_opening_job

"""## 3.4 Duplicated Companies

*   Exact company_name
*   Exact company_city
"""

company_df

# Convert 'company_name' to lowercase
company_df['company_name'] = company_df['company_name'].str.lower()

# Replace null values in 'company_city' with a placeholder value
company_df['company_city'] = company_df['company_city'].fillna('NULL')

# Group by 'company_name' and 'company_city' (replace 'company_city' with 'NULL' for null values)
company_df['company_combination_count'] = company_df.groupby(['company_name', 'company_city'])['company_name'].transform('count')

# Assign sequential numbers within each duplicate group
company_df['company_duplicated_check'] = company_df.groupby(['company_name', 'company_city']).cumcount() + 1

# For rows with a single occurrence, set 'company_duplicated_check' to 0
company_df.loc[company_df['company_combination_count'] == 1, 'company_duplicated_check'] = 0

# For companies in Banking and Insurance, will not check duplicated, set 'company_duplicated_check' to 0
company_df.loc[company_df['company_industry'].isin(['Banking', 'Insurance']), 'company_duplicated_check'] = 0

# Convert 'NULL' back to NaN in the 'company_city' column
company_df['company_city'] = company_df['company_city'].replace('NULL', np.nan)

company_df.head()

# draw a histogram of the age column
filtered_df = company_df[company_df['company_duplicated_check'] != 0]

filtered_df['company_duplicated_check'].hist()

# add labels and title
plt.xlabel('Duplication case')
plt.ylabel('Frequency')
plt.title('Distribution of Duplicated Companies')
plt.xlim(0, 100)
plt.ylim(0, 20)

"""## 3.5 Create level column - Check sale team jobs


*   Level 1 - Normal check
*   Level 2 - Include 80% Similarity in job_description

---


*   Sale - PIC email
*   Sale - Type
*   Sale - Reactivated_at


"""

# Create Level column
check_all_opening_job['Level'] = ''


# Condition to mark Level 1
condition_l1 = ((check_all_opening_job['partial_duplicated_check_same_district_sim80_mark'] == '') & (check_all_opening_job['partial_duplication_same_location_count_mark'] == 'x')) | ((check_all_opening_job['partial_duplicated_check_diff_district_sim80_mark'] == '') & (check_all_opening_job['partial_duplication_diff_location_count_mark'] == 'x'))


# Condition to mark Level 2
condition_l2 = (check_all_opening_job['partial_duplicated_check_same_district_sim80_mark'] == 'x') | (check_all_opening_job['partial_duplicated_check_diff_district_sim80_mark'] == 'x')


check_all_opening_job.loc[condition_l1, 'Level'] = 'Level 1'
check_all_opening_job.loc[condition_l2, 'Level'] = 'Level 2'

# Left join Outbound jobs with all_open_job

# Left join all_opening_job with outbound_jobs on the 'job_id' column
check_all_opening_job = check_all_opening_job.merge(outbound_jobs[['job_id', 'company_reactivated_at', 'pic_email', 'type']],
                                  on='job_id',
                                  how='left')

# Now, merged_df contains the combined data from both dataframes
# You can rename the columns if needed
check_all_opening_job.rename(columns={'company_reactivated_at': 'outbound_company_reactivated_at',
                          'pic_email': 'outbound_pic_email',
                          'type': 'outbound_type'}, inplace=True)

"""Create new column to mark the duplicated cases"""

check_all_opening_job['Duplicated Cases'] = ''

check_all_opening_job.loc[(check_all_opening_job['Level'] == 'Level 1') & (check_all_opening_job['partial_duplication_same_location_count_mark'] == 'x'), 'Duplicated Cases'] = check_all_opening_job['partial_duplicated_check_same_district']

check_all_opening_job.loc[(check_all_opening_job['Level'] == 'Level 1') & (check_all_opening_job['partial_duplication_diff_location_count_mark'] == 'x'), 'Duplicated Cases'] = check_all_opening_job['partial_duplicated_check_different_district']

check_all_opening_job.loc[(check_all_opening_job['Level'] == 'Level 2') & (check_all_opening_job['partial_duplicated_check_same_district_sim80_mark'] == 'x'), 'Duplicated Cases'] = check_all_opening_job['partial_duplicated_check_same_district_sim80']

check_all_opening_job.loc[(check_all_opening_job['Level'] == 'Level 2') & (check_all_opening_job['partial_duplicated_check_diff_district_sim80_mark'] == 'x'), 'Duplicated Cases'] = check_all_opening_job['partial_duplicated_check_diff_district_sim80']

"""# 4 . Output

```
# Total Jobs
# Duplicated Jobs
- # Exact Duplication
- # Partial Duplication (same location)
- # Partial Duplication (different location)
-- # Unique Jobs
% Job Duplication

# Total Companies Have Job Open
# Duplicated Companies
-- # Unique Companies
% Company Duplication
```

"""

# total_jobs = len(check_all_opening_job)
# print('# Total Jobs:', total_jobs)

# ########

# duplicated_jobs_count = len(
#     check_all_opening_job[
#         (check_all_opening_job['exactly_duplicated_check'] != 0) |
#         ((check_all_opening_job['partial_duplicated_check_same_district'] == 0) & (check_all_opening_job['partial_duplicated_check_different_district'] != 0)) |
#         ((check_all_opening_job['partial_duplicated_check_same_district'] != 0) & (check_all_opening_job['partial_duplicated_check_different_district'] == 0)) |
#         ((check_all_opening_job['partial_duplicated_check_same_district'] != 0) & (check_all_opening_job['partial_duplicated_check_different_district'] != 0))
#     ]
# )
# print('# Duplicated Jobs:', duplicated_jobs_count)


# ########
# exact_duplication_count = len(check_all_opening_job[check_all_opening_job['exactly_duplicated_check'] != 0])
# print('- # Exact Duplication:', exact_duplication_count)

# ########

# partial_duplication_same_location_count = len(check_all_opening_job[check_all_opening_job['partial_duplicated_check_same_district'] != 0])
# print('- # Partial Duplication (same district):', partial_duplication_same_location_count)

# ########
# partial_duplication_diff_location_count = len(check_all_opening_job[check_all_opening_job['partial_duplicated_check_different_district'] != 0])
# print('- # Partial Duplication (different district):', partial_duplication_diff_location_count)



# ########

# exact_unique_jobs_count = len(
#     check_all_opening_job[
#         (check_all_opening_job['exactly_duplicated_check'].isin([1]))
#     ]
# )
# sd_unique_jobs_count = len(
#     check_all_opening_job[
#         (check_all_opening_job['partial_duplicated_check_same_district'].isin([1]))
#     ]
# )
# dd_unique_jobs_count = len(
#     check_all_opening_job[
#         (check_all_opening_job['partial_duplicated_check_different_district'].isin([1]))
#     ]
# )


# unique_jobs_count = exact_unique_jobs_count + sd_unique_jobs_count + dd_unique_jobs_count



# print('-- # Unique Jobs:', unique_jobs_count)


# ########
# # Just for check back with # Duplicated Jobs number
# sum_duplicated_jobs = exact_duplication_count + partial_duplication_same_location_count + partial_duplication_diff_location_count
# print('Just for check back- # SUM duplication:', sum_duplicated_jobs)

# ########

# unique_jobs_count_total = len(
#     check_all_opening_job[
#         (check_all_opening_job['exactly_duplicated_check'].isin([0, 1])) &
#         (check_all_opening_job['partial_duplicated_check_same_district'].isin([0, 1])) &
#         (check_all_opening_job['partial_duplicated_check_different_district'].isin([0, 1]))
#     ]
# )
# print('# Unique Jobs Total:', unique_jobs_count_total)
# print('-------')


# hcm_total_jobs = len(
#     check_all_opening_job[check_all_opening_job['job_city'] == 'Thành phố Hồ Chí Minh']
#     )
# print('# HCM - Total Jobs:', hcm_total_jobs)


# hn_total_jobs = len(
#     check_all_opening_job[check_all_opening_job['job_city'] == 'Hà Nội']
#     )
# print('# Hà Nội - Total Jobs:', hn_total_jobs)

# Included 80% similarity check

print('Included 80% similarity check ')

total_jobs = len(check_all_opening_job)
print('# Total Jobs:', total_jobs)

########

duplicated_jobs_count = len(
    check_all_opening_job[
        (check_all_opening_job['exactly_duplication_mark'] == 'x') |
        (check_all_opening_job['partial_duplicated_check_same_district_sim80_mark'] == 'x') |
        (check_all_opening_job['partial_duplicated_check_diff_district_sim80_mark'] == 'x')
    ]
)
print('# Duplicated Jobs:', duplicated_jobs_count)


########
exact_duplication_count = len(check_all_opening_job[check_all_opening_job['exactly_duplication_mark'] == 'x'])
print('- # Exact Duplication:', exact_duplication_count)

########

partial_duplication_same_location_count = len(check_all_opening_job[check_all_opening_job['partial_duplicated_check_same_district_sim80_mark'] == 'x'])
print('- # Partial Duplication (same district):', partial_duplication_same_location_count)

########
partial_duplication_diff_location_count = len(check_all_opening_job[check_all_opening_job['partial_duplicated_check_diff_district_sim80_mark'] == 'x'])
print('- # Partial Duplication (different district):', partial_duplication_diff_location_count)



########

exact_unique_jobs_count = len(
    check_all_opening_job[
        (check_all_opening_job['exactly_duplicated_check'].isin([1]))
    ]
)
sd_unique_jobs_count = len(
    check_all_opening_job[
        (check_all_opening_job['partial_duplicated_check_same_district'].isin([1]))
    ]
)
dd_unique_jobs_count = len(
    check_all_opening_job[
        (check_all_opening_job['partial_duplicated_check_different_district'].isin([1]))
    ]
)


unique_jobs_count = exact_unique_jobs_count + sd_unique_jobs_count + dd_unique_jobs_count
print('-- # Unique Jobs:', unique_jobs_count)


########

unique_jobs_count_total = len(
    check_all_opening_job[
        (check_all_opening_job['exactly_duplicated_check'].isin([0, 1])) &
        (check_all_opening_job['partial_duplicated_check_same_district'].isin([0, 1])) &
        (check_all_opening_job['partial_duplicated_check_different_district'].isin([0, 1]))
    ]
)
print('# Unique Jobs Total:', unique_jobs_count_total)
print('-------')


hcm_total_jobs = len(
    check_all_opening_job[check_all_opening_job['job_city'] == 'Thành phố Hồ Chí Minh']
    )
print('# HCM - Total Jobs:', hcm_total_jobs)


hn_total_jobs = len(
    check_all_opening_job[check_all_opening_job['job_city'] == 'Hà Nội']
    )
print('# Hà Nội - Total Jobs:', hn_total_jobs)

total_companies = len(company_df)
print('# Total Companies Have Job Open:', total_companies)

########


duplicated_companies_count = len(
    company_df[
        (company_df['company_duplicated_check'] != 0)
    ]
)
print('# Duplicated Companies:', duplicated_companies_count)

########

unique_companiess_count = len(
    company_df[
        (company_df['company_duplicated_check'].isin([1]))
    ]
)
print('-- # Unique Companies:', unique_companiess_count)

duplicated_companies_list = company_df[company_df['company_duplicated_check'] != 0]
duplicated_companies_list

"""Drop unnecessary columns"""

columns_to_drop = ['exact_combination_count', 'exactly_duplicated_check', 'partial_sd_combination_count', 'partial_duplicated_check_same_district',
                  'partial_duplication_same_location_count_mark', 'partial_duplicated_check_same_district_sim80',
                  'partial_duplicated_check_same_district_sim80_mark', 'combination_count', 'partial_dd_combination_count',
                  'partial_duplicated_check_different_district', 'partial_duplication_diff_location_count_mark', 'partial_duplicated_check_diff_district_sim80', 'partial_duplicated_check_diff_district_sim80_mark']

check_all_opening_job = check_all_opening_job.drop(columns=columns_to_drop)

check_all_opening_job = check_all_opening_job.sort_values(
    by=[
        "company_name",
        "company_id",
        "category_l3",
        "job_type",
        "job_city",
        "job_district",
        "Level"
    ]
)

"""# 5 . Export to Google Sheets"""

from gspread_dataframe import set_with_dataframe
from datetime import date

# Job - Export to Google Sheets / Part 1 index

check_all_opening_job.reset_index(inplace=True)

rows = len(check_all_opening_job.axes[0])
cols = len(check_all_opening_job.axes[1])

# Job - Export to Google Sheets / Part 2 Export

job_today = 'Job /' + date.today().strftime("%m/%d/%y")

sheet = gc.open_by_key('1GJmKMx6DP5fDOZcqo1K1GypdGPWqZJ0RP2UCYENbkUY')
worksheet = sheet.add_worksheet(title=job_today, rows=rows, cols=cols)
set_with_dataframe(worksheet, check_all_opening_job)

# Company - Export to Google Sheets / Part 1 index
duplicated_companies_list.reset_index(inplace=True)

rows = len(duplicated_companies_list.axes[0])
cols = len(duplicated_companies_list.axes[1])

# Company - Export to Google Sheets / Part 2 Export
com_today = 'Company /' + date.today().strftime("%m/%d/%y")

sheet = gc.open_by_key('1GJmKMx6DP5fDOZcqo1K1GypdGPWqZJ0RP2UCYENbkUY')
worksheet = sheet.add_worksheet(title=com_today, rows=rows, cols=cols)
set_with_dataframe(worksheet, duplicated_companies_list)
