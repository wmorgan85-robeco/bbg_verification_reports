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

# DBTITLE 1,Run Utils
# MAGIC %run "./utils"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Imports and Helper Functions

# COMMAND ----------

# DBTITLE 1,Imports
import datetime
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# COMMAND ----------

# DBTITLE 1,Colour Scheme
robeco_palette = {
    "Blue": "#00A2BD",
    "Orange": "#F79608",
    "Pink": "#B50063",
}

# COMMAND ----------

# DBTITLE 1,Chart Helpers
def robeco_bar_chart(data, x=None, y=None, *, title="", xlabel="Month", ylabel="Count", color=None, figsize=(14, 6), stacked=False, legend_title=None, colors=None):
    """Standard Robeco-styled bar chart. For stacked charts, `colors` is an optional list (one per column)
    or dict (column -> colour); without it matplotlib's 10-colour cycle repeats after 10 series."""
    fig, ax = plt.subplots(figsize=figsize)
    if stacked:
        if isinstance(colors, dict):
            colors = [colors[c] for c in data.columns]
        data.plot.bar(stacked=True, ax=ax, width=0.85, color=colors)
    else:
        ax.bar(data[x], data[y], color=color or robeco_palette["Blue"])
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.tick_params(axis="x", rotation=45)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5, color="LightGrey")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_axisbelow(True)
    if legend_title:
        ax.legend(title=legend_title, bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
    plt.tight_layout()
    display(fig)
    plt.close(fig)


def robeco_dumbbell_chart(data, label_col, start_col, end_col, *, title="", start_label="Start", end_label="End", xlabel="Unique Securities", figsize=(12, None), log=False, flag_col=None):
    """Robeco-styled dumbbell chart: one row per label, a dot for start and end values joined by a line.
    log=True uses a log x-axis (products differ by orders of magnitude); flag_col bolds flagged rows."""
    data = data.sort_values(end_col).reset_index(drop=True)
    if log:  # zeros cannot be shown on a log axis; plot them at 1 (labels still show the true delta)
        data = data.assign(_s=data[start_col].clip(lower=1), _e=data[end_col].clip(lower=1))
    else:
        data = data.assign(_s=data[start_col], _e=data[end_col])
    height = figsize[1] or max(3, 0.45 * len(data) + 1.5)
    fig, ax = plt.subplots(figsize=(figsize[0], height))
    y = range(len(data))
    ax.hlines(y, data["_s"], data["_e"], color="LightGrey", linewidth=3, zorder=1)
    ax.scatter(data["_s"], y, color=robeco_palette["Blue"], s=60, label=start_label, zorder=2)
    ax.scatter(data["_e"], y, color=robeco_palette["Orange"], s=30, label=end_label, zorder=3)
    for i, row in data.iterrows():
        delta = row[end_col] - row[start_col]
        pct = f" ({delta / row[start_col]:+.1%})" if row[start_col] else " (new)"
        flagged = bool(flag_col and row[flag_col])
        colour = robeco_palette["Pink"] if flagged else "DimGrey"
        ax.annotate(
            f"{row[start_col]:,.0f} -> {row[end_col]:,.0f}   {delta:+,.0f}{pct}",
            (max(row["_s"], row["_e"]), i), xytext=(10, 0), textcoords="offset points",
            va="center", fontsize=8, color=colour, fontweight="bold" if flagged else "normal",
        )
    if log:
        ax.set_xscale("log")
        ax.set_xlim(right=data[["_s", "_e"]].max().max() * 8)  # room for labels
    ax.set_yticks(list(y))
    ax.set_yticklabels(data[label_col], fontsize=9)
    if flag_col:
        for tick, flagged in zip(ax.get_yticklabels(), data[flag_col]):
            if flagged:
                tick.set_color(robeco_palette["Pink"])
                tick.set_fontweight("bold")
    ax.set_title(title)
    ax.set_xlabel(xlabel)
    ax.xaxis.grid(True, linestyle="--", alpha=0.5, color="LightGrey")
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    ax.set_axisbelow(True)
    ax.legend(loc="lower right", fontsize=8)
    plt.tight_layout()
    display(fig)
    plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data

# COMMAND ----------

# DBTITLE 1,Load Tables
ss_df = (
    tables("bbg.verification_sched_summary", "rob6332").current()
    .withColumn("STATUS", F.when(col("TOTAL_USAGE") > 0, "ACTIVE").otherwise("INACTIVE"))
)

sd_df = (
    tables("bbg.verification_sched_detail", "rob6332").current()
    .withColumns(
        {
            "DATE": F.to_date(col("PROCESSED_TIME")),
            "PROCESS_ID": F.when(col("SERVICE") == "F", F.substring(col("OUTPUT_ID"), 1, 3)).otherwise("API"),
            "PROCESS_NAME": process_map[col("PROCESS_ID")],
            # calendar month in the same format as ss_df.REPORT_YR_MONTH so the two tables can be joined
            "REPORT_YR_MONTH": F.date_format(col("PROCESSED_TIME"), "yyyy/MM"),
            # environment from the OUTPUT_ID suffix of named jobs (e.g. BBP_Analytics_PROD -> PROD, PREP_DM_EQ_TST -> TST).
            # FTP files (B02_<date>_<time>.ENC) and web-service request ids (<n>-<n>.OUT) carry no environment -> N/A
            "ENV": F.coalesce(
                F.nullif(F.upper(F.regexp_extract(col("OUTPUT_ID"), r"(?i)_(PROD|PRD|UAT|ACC|TST|TEST|DEV)$", 1)), F.lit("")),
                F.lit("N/A"),
            ),
        }
    )
)

# billed requests only - the default basis for all trend analysis
sd_billed = sd_df.where(col("CTRB_BILLING") == "Y")

# COMMAND ----------

# DBTITLE 1,Report Parameters
# ACCOUNT_ID is a string column in both tables
MAIN_ACCOUNT = "301491"
PORT_ACCOUNT = "791626"
ACCOUNTS = {MAIN_ACCOUNT: "Main", PORT_ACCOUNT: "PORT"}

# Derive report month from latest data
REPORT_MONTH = (
  ss_df
  .select(F.max("REPORT_YR_MONTH").alias("latest"))
  .first()["latest"]
)

PRIOR_MONTH = (datetime.datetime.strptime(REPORT_MONTH, "%Y/%m").replace(day=1) - datetime.timedelta(days=1)).strftime("%Y/%m")

LOOKBACK_START = "2024-01-01"
LOOKBACK_MONTH = LOOKBACK_START[:7].replace("-", "/")  # same start in REPORT_YR_MONTH format

# month-over-month flag: both conditions must hold, so low-volume products don't trigger on small absolute moves
MOM_PCT_THRESHOLD = 0.10
MOM_ABS_THRESHOLD = 50

print(f"Report Month:   {REPORT_MONTH}")
print(f"Prior Month:    {PRIOR_MONTH}")
print(f"Main Account:   {MAIN_ACCOUNT}")
print(f"PORT Account:   {PORT_ACCOUNT}")
print(f"Lookback Start: {LOOKBACK_START}")

# COMMAND ----------

# DBTITLE 1,Core Metrics
# Shared metric tables used by the Executive Summary, Reconciliation, Month-over-Month and Job Attribution sections.
# Unit of billing = one (BILLABLE_PRODUCT, ID_BB_UNIQUE) pair per account per month.
_keys = ["ACCOUNT_ID", "REPORT_YR_MONTH", "BILLABLE_PRODUCT", "ID_BB_UNIQUE"]

# --- Billed securities from the summary report (= invoice basis) -------------------------------
billed_securities = (
    ss_df
    .where(col("ACCOUNT_ID").isin(list(ACCOUNTS)))
    .groupBy(_keys)  # a security can have several rows (e.g. qualifiers); ACTIVE if any row has usage
    .agg(
        F.max(col("TOTAL_USAGE") > 0).alias("IS_ACTIVE"),
        F.sum("GROUP_AC").alias("ACCESS"),
    )
    .withColumn("STATUS", F.when(col("IS_ACTIVE"), "ACTIVE").otherwise("INACTIVE"))
    .drop("IS_ACTIVE")
)

# --- Billed requests from the detail report, with the job that made them ------------------------
# JOB:
#   F (FTP)          OUTPUT_ID is a per-file name (B02_<date>_<time>.ENC) -> group by process, e.g. "FTP B02 Security";
#                    files outside the Bxx naming (BDL_OILANDGAS.CSV, History*.out) keep their OUTPUT_ID
#   H (Web Services) OUTPUT_ID is a per-request id (<n>-<n>.OUT) -> one bucket
#   other (e.g. P)   OUTPUT_ID is a named job (BBP_Analytics_PROD) -> use as is
NON_PROD_ENVS = ["UAT", "ACC", "TST", "TEST", "DEV"]

billed_requests = (
    sd_billed
    .where(col("ACCOUNT_ID").isin(list(ACCOUNTS)))
    .withColumn(
        "JOB",
        F.when(
            (col("SERVICE") == "F") & col("OUTPUT_ID").rlike(r"^B\d\d_") & col("PROCESS_NAME").isNotNull(),
            F.concat_ws(" ", F.lit("FTP"), col("PROCESS_ID"), col("PROCESS_NAME")),
        )
        .when(col("SERVICE") == "H", F.lit("Web Services (per request)"))
        .otherwise(col("OUTPUT_ID")),
    )
    .select(*_keys, "JOB", "SERVICE", "ENV")
    .distinct()
)

# --- Summary vs detail reconciliation ------------------------------------------------------------
# INACTIVE billed securities (no usage this month, carried from earlier downloads) have no detail rows, so:
#   MATCH  <=>  every ACTIVE summary security is in the detail (summary_only_active = 0)
#          and  every billed detail security is in the summary (detail_only = 0)
_sd_keys = billed_requests.select(_keys).distinct().withColumn("IN_DETAIL", F.lit(True))

recon_df = (
    billed_securities.withColumn("IN_SUMMARY", F.lit(True))
    .join(_sd_keys, _keys, "full_outer")
    .groupBy("ACCOUNT_ID", "REPORT_YR_MONTH", "BILLABLE_PRODUCT")
    .agg(
        F.count(F.when(col("IN_SUMMARY"), 1)).alias("billed_total"),
        F.count(F.when(col("STATUS") == "ACTIVE", 1)).alias("billed_active"),
        F.count(F.when(col("STATUS") == "INACTIVE", 1)).alias("billed_inactive"),
        F.coalesce(F.sum("ACCESS"), F.lit(0)).alias("access"),
        F.count(F.when(col("IN_DETAIL"), 1)).alias("detail_billed"),
        # billed but no billed request this month (carried over). Not always equal to billed_inactive:
        # STATUS comes from TOTAL_USAGE, which is 0 for some PORT securities that were requested.
        F.count(F.when(col("IN_SUMMARY") & col("IN_DETAIL").isNull(), 1)).alias("billed_not_requested"),
        F.count(F.when((col("STATUS") == "ACTIVE") & col("IN_DETAIL").isNull(), 1)).alias("summary_only_active"),
        F.count(F.when(col("IN_SUMMARY").isNull() & col("IN_DETAIL"), 1)).alias("detail_only"),
    )
    .withColumn("inactive_pct", F.round(col("billed_inactive") / col("billed_total"), 3))
    .withColumn("status", F.when((col("summary_only_active") == 0) & (col("detail_only") == 0), "MATCH").otherwise("MISMATCH"))
)

# --- Month-over-month change per account and product (REPORT_MONTH vs PRIOR_MONTH) ---------------
_cur = recon_df.where(col("REPORT_YR_MONTH") == REPORT_MONTH).select("ACCOUNT_ID", "BILLABLE_PRODUCT", col("billed_total").alias("current"))
_pri = recon_df.where(col("REPORT_YR_MONTH") == PRIOR_MONTH).select("ACCOUNT_ID", "BILLABLE_PRODUCT", col("billed_total").alias("prior"))

mom_df = (
    _cur.join(_pri, ["ACCOUNT_ID", "BILLABLE_PRODUCT"], "full_outer")
    .fillna(0, subset=["current", "prior"])
    .withColumn("delta", col("current") - col("prior"))
    .withColumn("pct", F.when(col("prior") > 0, F.round(col("delta") / col("prior"), 3)))
    .withColumn(
        "flag",
        ((F.abs(col("pct")) >= MOM_PCT_THRESHOLD) | col("pct").isNull()) & (F.abs(col("delta")) >= MOM_ABS_THRESHOLD),
    )
)

# --- Securities added / dropped between PRIOR_MONTH and REPORT_MONTH, attributed to jobs ----------
_sec_cur = billed_securities.where(col("REPORT_YR_MONTH") == REPORT_MONTH).select("ACCOUNT_ID", "BILLABLE_PRODUCT", "ID_BB_UNIQUE")
_sec_pri = billed_securities.where(col("REPORT_YR_MONTH") == PRIOR_MONTH).select("ACCOUNT_ID", "BILLABLE_PRODUCT", "ID_BB_UNIQUE")

def _with_jobs(securities, month, unattributed_label):
    """Attach the job(s) that requested each security in `month`; securities with no billed request get a label."""
    jobs = billed_requests.where(col("REPORT_YR_MONTH") == month).select("ACCOUNT_ID", "BILLABLE_PRODUCT", "ID_BB_UNIQUE", "JOB")
    return (
        securities.join(jobs, ["ACCOUNT_ID", "BILLABLE_PRODUCT", "ID_BB_UNIQUE"], "left")
        .withColumn("JOB", F.coalesce(col("JOB"), F.lit(unattributed_label)))
    )

added_df = _with_jobs(_sec_cur.subtract(_sec_pri), REPORT_MONTH, "Billed without request this month").withColumn("CHANGE", F.lit("added"))
dropped_df = _with_jobs(_sec_pri.subtract(_sec_cur), PRIOR_MONTH, "Carried over, no longer billed").withColumn("CHANGE", F.lit("dropped"))
changes_df = added_df.unionByName(dropped_df)

print(f"Core metrics built for {REPORT_MONTH} vs {PRIOR_MONTH}")

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

flags_df = (
    _recon_flags.unionByName(_mom_flags).unionByName(_access_flags).unionByName(_product_flags).unionByName(_nonprod_flags)
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
# MAGIC ### Usage Trends (Billed Only)
# MAGIC Unless stated otherwise, charts use billed requests (`CTRB_BILLING = "Y"`) from the detail report, counted as distinct securities per calendar month (which aligns with the billing month, see the reconciliation). Carried-over securities are not in the detail report, so these counts are below the invoice totals.

# COMMAND ----------

# DBTITLE 1,(1) Billed FTP Unique Securities Over Time (Main Account)
_pdf = (
  sd_billed
  .where(col("DATE") >= LOOKBACK_START)
  .where(col("SERVICE") == "F")
  .where(col("ACCOUNT_ID") == MAIN_ACCOUNT)
  .groupBy(col("REPORT_YR_MONTH").alias("report_yr_mth"))
  .agg(F.countDistinct("ID_BB_UNIQUE").alias("count"))
  .orderBy("report_yr_mth")
  .toPandas()
)

robeco_bar_chart(_pdf, x="report_yr_mth", y="count", title=f"Billed FTP Unique Securities Over Time - Main ({MAIN_ACCOUNT})")

# COMMAND ----------

# DBTITLE 1,Billed Unique Securities by Product Over Time (All Accounts)
_pdf = (
  sd_billed  # note: includes all accounts in the detail report, not only Main and PORT
  .where(col("DATE") >= LOOKBACK_START)
  .groupBy("BILLABLE_PRODUCT", col("REPORT_YR_MONTH").alias("report_yr_mth"))
  .agg(F.countDistinct("ID_BB_UNIQUE").alias("count"))
  .orderBy("report_yr_mth")
  .toPandas()
)

_pivot = _pdf.pivot(index="report_yr_mth", columns="BILLABLE_PRODUCT", values="count").fillna(0)

# Keep top N products by total volume, bucket rest as "Other"
top_n = 8
totals = _pivot.sum().sort_values(ascending=False)
top_cols = totals.head(top_n).index.tolist()
_pivot_grouped = _pivot[top_cols].copy()
_pivot_grouped["Other"] = _pivot.drop(columns=top_cols).sum(axis=1)

robeco_bar_chart(_pivot_grouped, stacked=True, title="Billed Unique Securities by Product Over Time (All Accounts)", figsize=(16, 7), legend_title="Billable Product")

# COMMAND ----------

# DBTITLE 1,Billed Unique Securities by Product - Monthly Table (Main Account)
(
  sd_billed
  .where(col("DATE") >= LOOKBACK_START)
  .where(col("ACCOUNT_ID") == MAIN_ACCOUNT)
  .groupBy(col("BILLABLE_PRODUCT"), col("REPORT_YR_MONTH").alias("report_yr_mth"))
  .agg(F.countDistinct("ID_BB_UNIQUE").alias("count"))
  .orderBy(col("report_yr_mth").asc(), col("count").desc())
  .display()
)

# COMMAND ----------

# DBTITLE 1,Billed vs Non-billed Unique Securities by Product - Monthly (Main Account)
(
  # monthly distinct securities; Y = billed, N = requested but not counted for billing (CTRB_BILLING = "N")
  sd_df
  .where(col("ACCOUNT_ID") == MAIN_ACCOUNT)
  .where(col("DATE") >= LOOKBACK_START)
  .groupBy("REPORT_YR_MONTH", "BILLABLE_PRODUCT")
  .pivot("CTRB_BILLING", ["Y", "N"])
  .agg(F.countDistinct("ID_BB_UNIQUE"))
  .fillna(0)
  .orderBy(col("REPORT_YR_MONTH").desc(), col("Y").desc())
  .display()
)

# COMMAND ----------

# DBTITLE 1,Billed Unique Securities by Job - Monthly Table (Main Account)
(
  # JOB groups per-file FTP OUTPUT_IDs by process (see Core Metrics); counts are (product, security) pairs
  billed_requests
  .where(col("ACCOUNT_ID") == MAIN_ACCOUNT)
  .where(col("REPORT_YR_MONTH") >= LOOKBACK_MONTH)
  .groupBy("REPORT_YR_MONTH", "JOB")
  .agg(F.countDistinct("BILLABLE_PRODUCT", "ID_BB_UNIQUE").alias("billed_pairs"))
  .orderBy(col("REPORT_YR_MONTH").desc(), col("billed_pairs").desc())
  .display()
)

# COMMAND ----------

# DBTITLE 1,(2) Billed Unique Securities by Product Over Time (PORT Account)
_pdf = (
  sd_billed
  .where(col("ACCOUNT_ID") == PORT_ACCOUNT)
  .where(col("DATE") >= LOOKBACK_START)
  .groupBy(col("REPORT_YR_MONTH").alias("report_yr_mth"), "BILLABLE_PRODUCT")
  .agg(F.countDistinct("ID_BB_UNIQUE").alias("count"))
  .orderBy("report_yr_mth", "BILLABLE_PRODUCT")
  .toPandas()
)

_pivot = _pdf.pivot(index="report_yr_mth", columns="BILLABLE_PRODUCT", values="count").fillna(0)

robeco_bar_chart(_pivot, stacked=True, title=f"Billed Unique Securities by Product Over Time - PORT ({PORT_ACCOUNT})", figsize=(16, 7), legend_title="Billable Product")

# COMMAND ----------

# DBTITLE 1,ListDer SecMaster - Billed Unique by Account (Summary Report)
(
  ss_df
  .where(col("BILLABLE_PRODUCT") == Billable_Products.LD_SECM)
  .where(col("REPORT_YR_MONTH") >= LOOKBACK_MONTH)
  .groupBy(col("BILLABLE_PRODUCT"), col("ACCOUNT_ID"), col("REPORT_YR_MONTH"))
  .agg(F.countDistinct(col("ID_BB_UNIQUE")))
  .orderBy(col("REPORT_YR_MONTH").asc())
  .display()
)

# COMMAND ----------

# DBTITLE 1,PORT Header
# MAGIC %md
# MAGIC ### PORT Account Analysis

# COMMAND ----------

# DBTITLE 1,PORT - Active Billed Securities by Product (Summary Report)
(
  ss_df
  .where(col("ACCOUNT_ID") == PORT_ACCOUNT)
  .where(col("STATUS") == "ACTIVE")
  .where(col("REPORT_YR_MONTH") >= LOOKBACK_MONTH)
  .groupBy(col("REPORT_YR_MONTH"), col("BILLABLE_PRODUCT"))
  .agg(F.countDistinct(col("ID_BB_UNIQUE")))
  .orderBy("REPORT_YR_MONTH")
  .display()
)

# COMMAND ----------

# DBTITLE 1,PORT - Billed Securities by Last Download Month (Report Month)
(
  ss_df
  .where(col("ACCOUNT_ID") == PORT_ACCOUNT)
  .where(col("REPORT_YR_MONTH") == REPORT_MONTH)
  .groupBy(col("YR_MONTH_OF_MOST_RECENT_DOWNLOAD"), col("STATUS"))
  .agg(F.countDistinct(col("ID_BB_UNIQUE")))
  .orderBy(col("YR_MONTH_OF_MOST_RECENT_DOWNLOAD"))
  .display()
)

# COMMAND ----------

# DBTITLE 1,(4) PORT FI SecMaster - Billed vs Non-billed by Month
(
  sd_df
  .where(col("ACCOUNT_ID") == PORT_ACCOUNT)
  .where(col("DATE") >= LOOKBACK_START)
  .where(col("BILLABLE_PRODUCT") == Billable_Products.FI_SECM)
  .groupBy("REPORT_YR_MONTH")
  .pivot("CTRB_BILLING", ["Y", "N"])
  .agg(F.countDistinct("ID_BB_UNIQUE"))
  .fillna(0)
  .orderBy("REPORT_YR_MONTH")
  .display()
)

# COMMAND ----------

# DBTITLE 1,Investigations Header
# MAGIC %md
# MAGIC ### Investigations
# MAGIC One-off deep dives. Dates and file names are fixed to the period investigated and are not driven by `REPORT_MONTH`.

# COMMAND ----------

# DBTITLE 1,(3) PORT Account Job Overview - FI SecMaster
# Investigation: which PORT jobs drive FI SecMaster billed volume, compared across specific snapshot dates (Aug-Nov 2025).
# Dates are fixed to that investigation; edit the list to re-run for another period.
required_data = (
  sd_df
  .where(col("ACCOUNT_ID") == PORT_ACCOUNT)
  .where(col("DATE").isin([
    "2025-08-01",
    "2025-09-01",
    # "2025-10-01",
    # "2025-10-15",
    # "2025-10-16",
    "2025-10-31",
    "2025-11-29"]))
  .where(col("BILLABLE_PRODUCT") == Billable_Products.FI_SECM)
  .groupBy("DATE", "OUTPUT_ID", "CTRB_BILLING").agg(F.countDistinct("ID_BB_UNIQUE").alias("Count"))
  .orderBy("DATE", "CTRB_BILLING")
)

(
  required_data
  .withColumn("env", F.element_at(F.split("output_id", "_"), -1))
  .groupBy("OUTPUT_ID", "CTRB_BILLING", "ENV")
  .pivot("date")
  .agg(F.first("count"))
  # .agg(F.sum("count"))
  .orderBy(
    "CTRB_BILLING", 
    F.when(F.lower(F.col("env")) == "dev", 1).when(F.lower(F.col("env")) == "uat", 2).when(F.lower(F.col("env")) == "prod", 3).otherwise(4), 
    "OUTPUT_ID")
).display()

# COMMAND ----------

# DBTITLE 1,Sanctions Issues
# Three B02 (Security Master) FTP deliveries in Aug 2025 were flagged by the
# sanctions screening process.  This query verifies which billable products
# and how many unique securities were affected per delivery file.
(
  sd_df
  .withColumn("OUTPUT_GROUP", F.substring(col("OUTPUT_ID"), 1, 3))
  .where(col("PROCESSED_TIME") > "2025-08-01")
  .where(col("SERVICE") == "F")
  .where(col("ACCOUNT_ID") == MAIN_ACCOUNT)
  .where(col("OUTPUT_GROUP") == "B02")
  .where(col("OUTPUT_ID").isin(
      "B02_20250818_144037.ENC",
      "B02_20250828_155507.ENC",
      "B02_20250829_150637.ENC",
  ))
  .groupBy(col("OUTPUT_ID"), col("BILLABLE_PRODUCT"))
  .agg(F.countDistinct("ID_BB_UNIQUE").alias("count"))
  .orderBy(col("OUTPUT_ID").asc(), col("BILLABLE_PRODUCT").asc())
  .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Appendix - Graveyard

# COMMAND ----------

# DBTITLE 1,Graveyard - FX Forward Inspection (Oct 2025)
if False:
  # One-off inspection: FX forwards on the PORT account, billed under Sched ListDer SecMaster (listed derivatives),
  # which is the expected classification.

  # FX Forward Usage Over Time - Active vs Inactive
  (
    ss_df
    .where(col("ACCOUNT_ID") == PORT_ACCOUNT)
    .where(col("BILLABLE_PRODUCT") == Billable_Products.LD_SECM)
    .groupBy(col("REPORT_YR_MONTH"), col("STATUS"))
    .agg(F.countDistinct(col("ID_BB_UNIQUE")))
    .orderBy(col("REPORT_YR_MONTH").asc())
    .display()
  )

  # FXF Job Over Time
  (
    sd_df
    .where(col("CTRB_BILLING") == "Y")
    .where(col("ACCOUNT_ID") == PORT_ACCOUNT)
    .where(col("OUTPUT_ID") == "BBG_FXF_Analytics_PROD")
    .where(col("BILLABLE_PRODUCT") == Billable_Products.LD_SECM)
    .withColumn("DOWNLOAD_MONTH", F.date_format(col("PROCESSED_TIME"), "yyyy-MM"))
    .groupBy(col("OUTPUT_ID"), col("DOWNLOAD_MONTH"))
    .agg(F.countDistinct(col("ID_BB_UNIQUE")))
    .orderBy(col("OUTPUT_ID"), col("DOWNLOAD_MONTH"))
    .display()
  )

  # Daily FXF Requests
  (
    sd_df
    .where(col("CTRB_BILLING") == "Y")
    .where(col("ACCOUNT_ID") == PORT_ACCOUNT)
    .where(col("OUTPUT_ID") == "BBG_FXF_Analytics_PROD")
    .groupBy(col("DATE"))
    .agg(F.count("*").alias("Count"))
    .orderBy(col("DATE").asc())
    .display()
  )