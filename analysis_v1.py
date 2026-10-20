# Databricks notebook source
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Bloomberg Data License - Monthly Billing Verification
# MAGIC
# MAGIC Verifies the Bloomberg scheduled-data invoice for the Main (301491) and PORT (791626) accounts, explains month-over-month changes and attributes billed securities to the jobs that request them. Re-run after each manual ingest; the report month is taken from the latest data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup

# COMMAND ----------

# DBTITLE 1,Run Report Core
# MAGIC %run "./report_core"

# COMMAND ----------

# DBTITLE 1,Executive Summary Header
# MAGIC %md
# MAGIC ### Executive Summary
# MAGIC Headline figures for the report month and rule-based flags. Flag rules:
# MAGIC * **MoM change**: billed unique securities for a product moved by at least 10% **and** at least 50 securities vs the prior month (the absolute floor avoids false positives on low-volume products)
# MAGIC * **MoM access change**: the same rule applied to access (`GROUP_AC`) per product
# MAGIC * **New / discontinued product**: a billable product appears or disappears vs the prior month
# MAGIC * **Non-prod billed**: billed requests from jobs whose `OUTPUT_ID` ends in a non-production suffix (UAT, ACC, TST, TEST, DEV)
# MAGIC * **Reconciliation**: summary vs detail check fails for a product
# MAGIC * **Incomplete data**: an account is missing the summary or detail file for the report month (bulk mid-month export not yet fully ingested)

# COMMAND ----------

# DBTITLE 1,Executive Summary - KPIs by Account
def _month_totals(month, suffix):
    return (
        recon_df.where(col("REPORT_YR_MONTH") == month)
        .groupBy("ACCOUNT_ID")
        .agg(
            F.sum("billed_total").alias(f"billed_unique{suffix}"),
            F.sum("access").alias(f"access{suffix}"),
            F.sum("billed_not_requested").alias(f"not_requested{suffix}"),
            F.count("*").alias(f"products{suffix}"),
            F.count(F.when(col("status") == "MISMATCH", 1)).alias(f"recon_mismatches{suffix}"),
        )
    )

kpi_df = (
    _month_totals(REPORT_MONTH, "")
    .join(_month_totals(PRIOR_MONTH, "_prior"), "ACCOUNT_ID", "left")
    .withColumn("ACCOUNT", F.create_map([x for k, v in ACCOUNTS.items() for x in (F.lit(k), F.lit(v))])[col("ACCOUNT_ID")])
    .select(
        "ACCOUNT", "ACCOUNT_ID",
        F.lit(REPORT_MONTH).alias("month"),
        "billed_unique", "billed_unique_prior",
        (col("billed_unique") - col("billed_unique_prior")).alias("billed_unique_delta"),
        F.round(col("billed_unique") / col("billed_unique_prior") - 1, 3).alias("billed_unique_pct"),
        "access", "access_prior",
        F.round(col("access") / col("access_prior") - 1, 3).alias("access_pct"),
        F.round(col("not_requested") / col("billed_unique"), 3).alias("carried_over_share"),
        "products",
        F.when(col("recon_mismatches") == 0, "PASS").otherwise("FAIL").alias("reconciliation"),
    )
    .orderBy("ACCOUNT_ID")
)
kpi_df.display()

# COMMAND ----------

# DBTITLE 1,Executive Summary - Flags
_mom_flags = (
    mom_df.where(col("flag") & (col("prior") > 0) & (col("current") > 0))
    .select(
        F.lit("MoM change").alias("rule"), "ACCOUNT_ID", col("BILLABLE_PRODUCT").alias("item"),
        F.format_string("%,d -> %,d (%+,d, %+.1f%%)", "prior", "current", "delta", col("pct") * 100).alias("detail"),
        F.abs(col("delta")).alias("_size"),
    )
)

# Access (GROUP_AC) is billed separately from unique securities, so apply the same MoM rule to it
_acc_cur = recon_df.where(col("REPORT_YR_MONTH") == REPORT_MONTH).select("ACCOUNT_ID", "BILLABLE_PRODUCT", col("access").alias("current"))
_acc_pri = recon_df.where(col("REPORT_YR_MONTH") == PRIOR_MONTH).select("ACCOUNT_ID", "BILLABLE_PRODUCT", col("access").alias("prior"))
_access_flags = (
    _acc_cur.join(_acc_pri, ["ACCOUNT_ID", "BILLABLE_PRODUCT"], "inner")
    .withColumn("delta", col("current") - col("prior"))
    .withColumn("pct", F.when(col("prior") > 0, col("delta") / col("prior")))
    .where((F.abs(col("pct")) >= MOM_PCT_THRESHOLD) & (F.abs(col("delta")) >= MOM_ABS_THRESHOLD))
    .select(
        F.lit("MoM access change").alias("rule"), "ACCOUNT_ID", col("BILLABLE_PRODUCT").alias("item"),
        F.format_string("%,d -> %,d (%+,d, %+.1f%%)", "prior", "current", "delta", col("pct") * 100).alias("detail"),
        F.abs(col("delta")).alias("_size"),
    )
)

_product_flags = (
    mom_df.where((col("prior") == 0) | (col("current") == 0))
    .select(
        F.when(col("prior") == 0, "New product").otherwise("Discontinued product").alias("rule"),
        "ACCOUNT_ID", col("BILLABLE_PRODUCT").alias("item"),
        F.format_string("%,d -> %,d", "prior", "current").alias("detail"),
        F.abs(col("delta")).alias("_size"),
    )
)

_nonprod_flags = (
    billed_requests
    .where(col("REPORT_YR_MONTH") == REPORT_MONTH)
    .where(col("ENV").isin(NON_PROD_ENVS))
    .groupBy("ACCOUNT_ID", "JOB", "ENV")
    .agg(F.count("*").alias("pairs"), F.collect_set("BILLABLE_PRODUCT").alias("products"))
    .select(
        F.lit("Non-prod billed").alias("rule"), "ACCOUNT_ID", col("JOB").alias("item"),
        F.format_string("%s: %,d billed (product, security) pairs in %s", "ENV", "pairs", F.array_join("products", ", ")).alias("detail"),
        col("pairs").alias("_size"),
    )
)

_recon_flags = (
    recon_df.where((col("REPORT_YR_MONTH") == REPORT_MONTH) & (col("status") == "MISMATCH"))
    .select(
        F.lit("Reconciliation").alias("rule"), "ACCOUNT_ID", col("BILLABLE_PRODUCT").alias("item"),
        F.format_string("active not in detail: %,d; detail not in summary: %,d", "summary_only_active", "detail_only").alias("detail"),
        (col("summary_only_active") + col("detail_only")).alias("_size"),
    )
)

# Completeness: flag any account that is missing summary or detail data for the report month
_completeness_rows = []
for _acct, _info in COMPLETENESS.items():
    if _info["status"] != "PASS":
        _completeness_rows.append((_acct, ", ".join(_info["missing"])))
if _completeness_rows:
    _completeness_flags = spark.createDataFrame(_completeness_rows, ["ACCOUNT_ID", "_missing"]).select(
        F.lit("Incomplete data").alias("rule"), "ACCOUNT_ID",
        F.lit(REPORT_MONTH).alias("item"),
        F.concat(F.lit("Missing: "), col("_missing")).alias("detail"),
        F.lit(9999).alias("_size"),
    )
else:
    _completeness_flags = spark.createDataFrame([], "rule: string, ACCOUNT_ID: string, item: string, detail: string, _size: int")

flags_df = (
    _completeness_flags.unionByName(_recon_flags).unionByName(_mom_flags).unionByName(_access_flags).unionByName(_product_flags).unionByName(_nonprod_flags)
    .orderBy("rule", "ACCOUNT_ID", col("_size").desc())
    .drop("_size")
)

print(f"{flags_df.count()} flag(s) for {REPORT_MONTH}")
flags_df.display()

# COMMAND ----------

# DBTITLE 1,Analyst Notes
# MAGIC %md
# MAGIC #### Analyst notes
# MAGIC _Fill in per report month: explanation of flags, actions agreed, owners._
# MAGIC
# MAGIC * 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Billing Reconciliation

# COMMAND ----------

# DBTITLE 1,Billing Reconciliation - Invoice Figures by Account
# One table per account for REPORT_MONTH. These figures match the invoice.
for account_id, account_name in ACCOUNTS.items():
    print(f"{account_name} account ({account_id}) - {REPORT_MONTH}")
    (
        ss_df
        .where(col("REPORT_YR_MONTH") == REPORT_MONTH)
        .where(col("ACCOUNT_ID") == account_id)
        .groupBy("BILLABLE_PRODUCT")
        .agg(
            F.countDistinct("ID_BB_UNIQUE").alias("Unique"),  # this matches the unique billed
            F.sum("GROUP_AC").alias("Access"),                # this matches the access billed
        )
        .orderBy(col("Unique").desc())
        .display()
    )

# COMMAND ----------

# DBTITLE 1,Billing Reconciliation - Summary vs Detail Check
# Billed unique securities in the summary report (= invoice) vs distinct billed securities
# (CTRB_BILLING = "Y") in the detail report for the same calendar month. recon_df is built in Core Metrics.
# billed_inactive = billed but not downloaded this month; these have no detail rows by definition.
recon_df.where(col("REPORT_YR_MONTH") == REPORT_MONTH).orderBy("ACCOUNT_ID", col("billed_total").desc()).display()

# History: count of product/account combinations that fail the check, per month
(
    recon_df
    .groupBy("REPORT_YR_MONTH")
    .agg(
        F.count("*").alias("products_checked"),
        F.count(F.when(col("status") == "MISMATCH", 1)).alias("mismatches"),
        F.sum("summary_only_active").alias("summary_only_active"),
        F.sum("detail_only").alias("detail_only"),
    )
    .orderBy(col("REPORT_YR_MONTH").desc())
    .display()
)

# COMMAND ----------

# DBTITLE 1,Month-over-Month Header
# MAGIC %md
# MAGIC ### Month-over-Month Change
# MAGIC Billed unique securities per product, prior month vs report month. The driver table shows which jobs added or dropped billed securities. Securities billed without a request in the month are carried over from earlier downloads.

# COMMAND ----------

# DBTITLE 1,MoM Dumbbell by Account
_mom_pdf = mom_df.toPandas()

for account_id, account_name in ACCOUNTS.items():
    _acc = _mom_pdf[_mom_pdf["ACCOUNT_ID"] == account_id]
    robeco_dumbbell_chart(
        _acc, "BILLABLE_PRODUCT", "prior", "current",
        title=f"Billed Unique Securities by Product - {account_name} ({account_id})",
        start_label=PRIOR_MONTH, end_label=REPORT_MONTH,
        log=True, flag_col="flag",  # pink = flagged (>= 10% and >= 50 securities)
    )

# COMMAND ----------

# DBTITLE 1,MoM Drivers by Job
# Net change in billed (product, security) pairs per job. A security requested by several jobs is counted for each,
# so job rows can sum to more than the product-level delta; the 'net_unique' column is the de-duplicated total.
(
    changes_df
    .groupBy("ACCOUNT_ID", "JOB")
    .agg(
        F.countDistinct(F.when(col("CHANGE") == "added", F.concat_ws("|", "BILLABLE_PRODUCT", "ID_BB_UNIQUE"))).alias("added"),
        F.countDistinct(F.when(col("CHANGE") == "dropped", F.concat_ws("|", "BILLABLE_PRODUCT", "ID_BB_UNIQUE"))).alias("dropped"),
        F.collect_set("BILLABLE_PRODUCT").alias("products"),
    )
    .withColumn("net", col("added") - col("dropped"))
    .orderBy("ACCOUNT_ID", F.abs(col("net")).desc())
    .display()
)

# De-duplicated totals per account and product (reconciles to the dumbbell deltas)
(
    changes_df
    .select("ACCOUNT_ID", "BILLABLE_PRODUCT", "ID_BB_UNIQUE", "CHANGE").distinct()
    .groupBy("ACCOUNT_ID", "BILLABLE_PRODUCT")
    .pivot("CHANGE", ["added", "dropped"]).count()
    .fillna(0)
    .withColumn("net_unique", col("added") - col("dropped"))
    .orderBy("ACCOUNT_ID", F.abs(col("net_unique")).desc())
    .display()
)

# COMMAND ----------

# DBTITLE 1,Job Attribution Header
# MAGIC %md
# MAGIC ### Job Attribution
# MAGIC Which jobs drive the billed securities in the report month. FTP deliveries are grouped by process (B01, B02, ...), web-service requests by `OUTPUT_ID`.
# MAGIC * **requested**: billed (product, security) pairs the job requested this month
# MAGIC * **exclusive**: pairs requested *only* by this job, i.e. the billed volume that would disappear this month if the job had not run (it may still be billed later as INACTIVE)
# MAGIC * **Shared** (requested by more than one job) and **Not requested** (billed but carried over from earlier months) rows close the gap, so the `exclusive` column sums to the billed total per account
# MAGIC * Web-service `OUTPUT_ID`s are per-request ids, so all web-service usage is one bucket

# COMMAND ----------

# DBTITLE 1,Job Attribution - Report Month
_req = billed_requests.where(col("REPORT_YR_MONTH") == REPORT_MONTH)

# number of distinct jobs requesting each billed pair
_req_n = _req.groupBy("ACCOUNT_ID", "BILLABLE_PRODUCT", "ID_BB_UNIQUE").agg(F.countDistinct("JOB").alias("N_JOBS"))

job_attr_df = (
    _req.join(_req_n, ["ACCOUNT_ID", "BILLABLE_PRODUCT", "ID_BB_UNIQUE"])
    .groupBy("ACCOUNT_ID", "JOB")
    .agg(
        F.first("ENV").alias("ENV"),
        F.count("*").alias("requested"),
        F.count(F.when(col("N_JOBS") == 1, 1)).alias("exclusive"),
        F.countDistinct("BILLABLE_PRODUCT").alias("n_products"),
    )
)

_shared = (
    _req_n.where(col("N_JOBS") > 1)
    .groupBy("ACCOUNT_ID").agg(F.count("*").alias("exclusive"))
    .withColumn("JOB", F.lit("Shared (multiple jobs)"))
)
_inactive = (
    recon_df.where(col("REPORT_YR_MONTH") == REPORT_MONTH)
    .groupBy("ACCOUNT_ID").agg(F.sum("billed_not_requested").alias("exclusive"))
    .withColumn("JOB", F.lit("Not requested (carried over)"))
)

_attr = job_attr_df.unionByName(_shared.unionByName(_inactive), allowMissingColumns=True)
_totals = recon_df.where(col("REPORT_YR_MONTH") == REPORT_MONTH).groupBy("ACCOUNT_ID").agg(F.sum("billed_total").alias("billed_total"))

(
    _attr.join(_totals, "ACCOUNT_ID")
    .withColumn("exclusive_share", F.round(col("exclusive") / col("billed_total"), 3))
    .select("ACCOUNT_ID", "JOB", "ENV", "requested", "exclusive", "exclusive_share", "n_products")
    .orderBy("ACCOUNT_ID", col("exclusive").desc())
    .display()
)

# COMMAND ----------

# DBTITLE 1,Job Attribution Over Time (Billed, Exclusive)
# Monthly billed pairs per job: exclusive to one job, shared across jobs, or not requested that month (carried over)
_req_all = billed_requests.where(col("REPORT_YR_MONTH") >= LOOKBACK_MONTH)
_n_all = _req_all.groupBy(_keys).agg(F.countDistinct("JOB").alias("N_JOBS"), F.first("JOB").alias("JOB"))

_attr_month = (
    _n_all
    .withColumn("BUCKET", F.when(col("N_JOBS") == 1, col("JOB")).otherwise(F.lit("Shared (multiple jobs)")))
    .groupBy("ACCOUNT_ID", "REPORT_YR_MONTH", "BUCKET").agg(F.count("*").alias("count"))
    .unionByName(
        recon_df
        .where(col("REPORT_YR_MONTH") >= LOOKBACK_MONTH)
        .groupBy("ACCOUNT_ID", "REPORT_YR_MONTH").agg(F.sum("billed_not_requested").alias("count"))
        .withColumn("BUCKET", F.lit("Not requested (carried over)"))
    )
    .toPandas()
)

top_n = 8
# distinct colours for individual jobs; fixed colours for the catch-all buckets so they never collide with a job
_job_palette = [plt.get_cmap("tab20").colors[i] for i in (0, 4, 6, 8, 10, 12, 16, 18)]
_bucket_colours = {
    "Other jobs": "Silver",
    "Shared (multiple jobs)": robeco_palette["Orange"],
    "Not requested (carried over)": "DimGrey",
}

for account_id, account_name in ACCOUNTS.items():
    _pivot = (
        _attr_month[_attr_month["ACCOUNT_ID"] == account_id]
        .pivot(index="REPORT_YR_MONTH", columns="BUCKET", values="count").fillna(0).sort_index()
    )
    _fixed = [c for c in ["Shared (multiple jobs)", "Not requested (carried over)"] if c in _pivot.columns]
    _jobs = _pivot.drop(columns=_fixed)
    _top = _jobs.sum().sort_values(ascending=False).head(top_n).index.tolist()
    _grouped = _jobs[_top].copy()
    if len(_jobs.columns) > top_n:
        _grouped["Other jobs"] = _jobs.drop(columns=_top).sum(axis=1)
    for c in _fixed:
        _grouped[c] = _pivot[c]
    _colours = {job: _job_palette[i] for i, job in enumerate(_top)} | _bucket_colours
    robeco_bar_chart(
        _grouped, stacked=True, figsize=(16, 7), legend_title="Job", colors=_colours,
        title=f"Billed Unique Securities by Job - {account_name} ({account_id})",
    )

# COMMAND ----------

# DBTITLE 1,Usage Trends Header
# MAGIC %md
# MAGIC ### Usage Trends
# MAGIC All charts and tables loop over both accounts on the same basis:
# MAGIC * **Stacked bar and monthly table**: billed requests from the detail report (`CTRB_BILLING = "Y"`), counted as distinct securities per calendar month. Carried-over securities are not in the detail, so these counts are below the invoice totals.
# MAGIC * **Billed vs non-billed**: all requests from the detail report (Y and N), to show what was requested but not billed.
# MAGIC * **By job**: billed (product, security) pairs per job from the detail report.

# COMMAND ----------

# DBTITLE 1,Billed Unique Securities by Product Over Time
# Stacked bar chart: billed unique securities by product per month, one chart per account.
for account_id, account_name in ACCOUNTS.items():
    _pdf = (
        sd_billed
        .where(col("DATE") >= LOOKBACK_START)
        .where(col("ACCOUNT_ID") == account_id)
        .groupBy("BILLABLE_PRODUCT", col("REPORT_YR_MONTH").alias("report_yr_mth"))
        .agg(F.countDistinct("ID_BB_UNIQUE").alias("count"))
        .orderBy("report_yr_mth")
        .toPandas()
    )
    _pivot = _pdf.pivot(index="report_yr_mth", columns="BILLABLE_PRODUCT", values="count").fillna(0)
    top_n = 8
    totals = _pivot.sum().sort_values(ascending=False)
    top_cols = totals.head(top_n).index.tolist()
    _pivot_grouped = _pivot[top_cols].copy()
    if len(_pivot.columns) > top_n:
        _pivot_grouped["Other"] = _pivot.drop(columns=top_cols).sum(axis=1)
    robeco_bar_chart(
        _pivot_grouped, stacked=True, figsize=(16, 7), legend_title="Billable Product",
        title=f"Billed Unique Securities by Product Over Time - {account_name} ({account_id})",
    )

# COMMAND ----------

# DBTITLE 1,Billed Unique Securities by Product - Monthly Table
# Monthly detail table: billed unique securities by product, one table per account.
for account_id, account_name in ACCOUNTS.items():
    print(f"\n{account_name} ({account_id})")
    (
        sd_billed
        .where(col("DATE") >= LOOKBACK_START)
        .where(col("ACCOUNT_ID") == account_id)
        .groupBy("BILLABLE_PRODUCT", col("REPORT_YR_MONTH").alias("report_yr_mth"))
        .agg(F.countDistinct("ID_BB_UNIQUE").alias("count"))
        .orderBy(col("report_yr_mth").asc(), col("count").desc())
        .display()
    )

# COMMAND ----------

# DBTITLE 1,Billed vs Non-billed by Product - Monthly
# Monthly Y/N pivot: billed (CTRB_BILLING=Y) vs requested-but-not-billed (N), one table per account.
for account_id, account_name in ACCOUNTS.items():
    print(f"\n{account_name} ({account_id})")
    (
        sd_df
        .where(col("ACCOUNT_ID") == account_id)
        .where(col("DATE") >= LOOKBACK_START)
        .groupBy("REPORT_YR_MONTH", "BILLABLE_PRODUCT")
        .pivot("CTRB_BILLING", ["Y", "N"])
        .agg(F.countDistinct("ID_BB_UNIQUE"))
        .fillna(0)
        .orderBy(col("REPORT_YR_MONTH").desc(), col("Y").desc())
        .display()
    )

# COMMAND ----------

# DBTITLE 1,Billed Unique Securities by Job - Monthly Table
# Monthly billed (product, security) pairs per job, one table per account.
for account_id, account_name in ACCOUNTS.items():
    print(f"\n{account_name} ({account_id})")
    (
        billed_requests
        .where(col("ACCOUNT_ID") == account_id)
        .where(col("REPORT_YR_MONTH") >= LOOKBACK_MONTH)
        .groupBy("REPORT_YR_MONTH", "JOB")
        .agg(F.countDistinct("BILLABLE_PRODUCT", "ID_BB_UNIQUE").alias("billed_pairs"))
        .orderBy(col("REPORT_YR_MONTH").desc(), col("billed_pairs").desc())
        .display()
    )

# COMMAND ----------

# DBTITLE 1,Carried-Over Securities Header
# MAGIC %md
# MAGIC ### Carried-Over Securities
# MAGIC Billed securities by last download month, bucketed by age relative to the report month. Based on the summary report (= invoice basis). Shows how stale the carried-over portion is.

# COMMAND ----------

# DBTITLE 1,Carried-Over Securities Age by Account
# Age distribution of billed securities by last download month, relative to REPORT_MONTH.
_report_dt = datetime.datetime.strptime(REPORT_MONTH, "%Y/%m")
_report_yr, _report_mo = _report_dt.year, _report_dt.month

# Deduplicate to one row per (account, product, security), taking the most recent download month.
# Without this, a security with multiple qualifiers and different download months would be counted in two buckets.
_age = (
    ss_df
    .where(col("REPORT_YR_MONTH") == REPORT_MONTH)
    .groupBy("ACCOUNT_ID", "BILLABLE_PRODUCT", "ID_BB_UNIQUE")
    .agg(F.max("YR_MONTH_OF_MOST_RECENT_DOWNLOAD").alias("YR_MONTH_OF_MOST_RECENT_DOWNLOAD"))
    .withColumn("_dl_yr", F.substring("YR_MONTH_OF_MOST_RECENT_DOWNLOAD", 1, 4).cast("int"))
    .withColumn("_dl_mo", F.substring("YR_MONTH_OF_MOST_RECENT_DOWNLOAD", 6, 2).cast("int"))
    .withColumn("_months_ago", (F.lit(_report_yr) - col("_dl_yr")) * 12 + F.lit(_report_mo) - col("_dl_mo"))
    .withColumn(
        "age_bucket",
        F.when(col("_months_ago") == 0, "0  This month")
        .when(col("_months_ago") == 1, "1  1 month ago")
        .when(col("_months_ago") == 2, "2  2 months ago")
        .when(col("_months_ago") <= 5, "3  3-5 months ago")
        .when(col("_months_ago") <= 11, "4  6-11 months ago")
        .otherwise("5  12+ months ago")
    )
)

for account_id, account_name in ACCOUNTS.items():
    print(f"\n{account_name} ({account_id}) - {REPORT_MONTH}")
    (
        _age
        .where(col("ACCOUNT_ID") == account_id)
        .groupBy("age_bucket")
        .agg(F.count("*").alias("billed_pairs"))
        .orderBy("age_bucket")
        .withColumn("age_bucket", F.regexp_replace("age_bucket", r"^\d  ", ""))
        .display()
    )