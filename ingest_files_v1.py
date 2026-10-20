# Databricks notebook source
# MAGIC %md
# MAGIC # Setup

# COMMAND ----------

# DBTITLE 1,Widget Setup
# Databricks notebook source
dbutils.widgets.text("now", defaultValue="0", label="now")
dbutils.widgets.text("env", defaultValue="", label="env")
dbutils.widgets.text("user", defaultValue="", label="user")
dbutils.widgets.text("anchor_date", defaultValue="", label="anchor_date")
dbutils.widgets.dropdown("autocopy", "false", ["false", "true"], "autocopy")
dbutils.widgets.dropdown("rollbacktonow", "false", ["false", "true"], "rollbacktonow")
dbutils.widgets.dropdown("update", "full", ["full", "recent"], "update period")

# COMMAND ----------

_now = int(dbutils.widgets.get("now"))
_env = dbutils.widgets.get("env")
_update = dbutils.widgets.get("update")
_autocopy = dbutils.widgets.get("autocopy").lower() == "true"
_rollbacktonow = dbutils.widgets.get("rollbacktonow").lower() == "true"
_user = dbutils.widgets.get("user")

# COMMAND ----------

# DBTITLE 1,Utils Setup
# MAGIC %run "./utils"

# COMMAND ----------

# MAGIC %md
# MAGIC # Imports

# COMMAND ----------

# DBTITLE 1,Imports
from io import BytesIO
import hashlib
import tarfile
import urllib3
import uuid

from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    LongType, TimestampType
)

# Disable SSL warnings from urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from enum import Enum
class Processing_Status(str, Enum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"         # already processed, no action taken
    OVERRIDE = "OVERRIDE"       # manually excluded or overriden

# COMMAND ----------

# DBTITLE 1,Constants
STORAGE_ACCOUNT = "rbqrrhippoprd"
CONTAINER = "datascientist"
BASE_PATH = "users/mw/bbg_verification_reports"
INCOMING = f"{BASE_PATH}/incoming"
ARCHIVE = f"{BASE_PATH}/archive"
EXCLUDED = f"{BASE_PATH}/excluded"

# non-bval accounts
KNOWN_ACCOUNTS = {"301491", "791626"}

# COMMAND ----------

# MAGIC %md
# MAGIC # Table Creation & Maintenance

# COMMAND ----------

# DBTITLE 1,Drop Tables
# drop_process_log = True
# drop_sched_detail = True
# drop_sched_summary = True

drop_process_log = False
drop_sched_detail = False
drop_sched_summary = False

if drop_process_log:
  tables.drop("bbg.verification_file_process_log")

if drop_sched_detail:
    tables.drop("bbg.verification_sched_detail")

if drop_sched_summary:
    tables.drop("bbg.verification_sched_summary")

# COMMAND ----------

# DBTITLE 1,Table Schemas
# File Processing Table
try:
    process_log_table = tables.create(
        "bbg.verification_file_process_log",
        """
        file_name string *
        process_date timestamp *d
        file_size long
        file_checksum string
        process_status string *
        error_message string
        """,
        TableTypes.FactTable
    )
except Exception:
    process_log_table = tables("bbg.verification_file_process_log")

# IMPORTANT: must match the tables.create() definition above
process_log_schema = StructType([  
    StructField("file_name", StringType(), nullable=False),  
    StructField("process_date", TimestampType(), nullable=False),  
    StructField("file_size", LongType(), nullable=False),  
    StructField("file_checksum", StringType(), nullable=False),  
    StructField("process_status", StringType(), nullable=False),  
    StructField("error_message", StringType(), nullable=True),  
])

# Scheduled Detail Table
try:
    sched_detail_table = tables.create(
        "bbg.verification_sched_detail",
        """
        account_id string *
        output_id string *
        security string *
        id_bb_unique string *
        qualifier string *
        processed_time timestamp *d
        download_product string *
        billable_product string *
        service string
        ctrb_billing string *
        ctrb_ac string *
        bo_optimized string *
        ctrb_bac string *
        request_count int
        features string
        """
    )
except Exception:
    sched_detail_table = tables("bbg.verification_sched_detail")

# IMPORTANT: must match the tables.create() definition above
# sched_detail_schema = StructType([
#     StructField("account_id", StringType(), nullable=False),
#     StructField("output_id", StringType(), nullable=False),
#     StructField("security", StringType(), nullable=False),
#     StructField("id_bb_unique", StringType(), nullable=False),
#     StructField("qualifier", StringType(), nullable=False),
#     StructField("processed_time", TimestampType(), nullable=False),
#     StructField("download_product", StringType(), nullable=False),
#     StructField("billable_product", StringType(), nullable=False),
#     StructField("service", StringType(), nullable=False),
#     StructField("ctrb_billing", StringType(), nullable=False),
#     StructField("ctrb_ac", StringType(), nullable=False),
#     StructField("bo_optimized", StringType(), nullable=False),
#     StructField("ctrb_bac", StringType(), nullable=False),
#     StructField("request_count", IntegerType(), nullable=False),
#     StructField("features", StringType(), nullable=True)
# ])

# Scheduled Summary Table
try:
    sched_summary_table = tables.create(
        "bbg.verification_sched_summary",
        """
        account_id string *
        report_yr_month string *
        id_bb_unique string *
        qualifier string *
        yr_month_of_most_recent_download string
        billable_product string *
        num_of_accounts int
        total_usage int
        client_usage int
        group_ac int
        client_bo_opt_ac int
        """
    )
except Exception:
    sched_summary_table = tables("bbg.verification_sched_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC # Workflow

# COMMAND ----------

# DBTITLE 1,Small Utilities
def get_file_bytesio(file_name: str):
    """
    Looks for files in the incoming directory
    """
    fp = tables.functions.filesparser
    return BytesIO(fp.read_file_and_process(
        STORAGE_ACCOUNT, 
        CONTAINER, 
        f"{INCOMING}/",
        file_name
    ))

def calculate_checksum(bytes_io_obj: BytesIO) -> str:
    sha256_hash = hashlib.sha256()
    bytes_io_obj.seek(0)
    for byte_block in iter(lambda: bytes_io_obj.read(4096), b""):
        sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def get_bytesio_size(bytes_io_obj) -> int:
    current_pos = bytes_io_obj.tell()
    bytes_io_obj.seek(0, 2)
    size = bytes_io_obj.tell()
    bytes_io_obj.seek(current_pos)
    return size

def is_processed(file_name: str, file_checksum: str) -> bool:
    return process_log_table.current().where(
        (col("file_name") == file_name)
        & (col("file_checksum") == file_checksum)
        & (col("process_status") == Processing_Status.COMPLETED.value)
    ).count() > 0

def log_status(
    file_name: str, 
    file_size: int, 
    file_checksum: str, 
    status: Processing_Status, 
    error_message: str = None
):
    try:
        df = spark.createDataFrame([{
            "file_name": file_name,
            "process_date": tables.timestamp(),
            "file_size": file_size,
            "file_checksum": file_checksum,
            "process_status": status.value,
            "error_message": error_message
        }], schema=process_log_schema)

        print(f"Logging status for file {file_name} with size {file_size} bytes and checksum {file_checksum} as {status.value}")

        process_log_table.update(df)
        tables.bump()
    except Exception as e:
        print(f"Failed to log status for file {file_name}: {str(e)}")
        raise Exception(f"Error logging status for file: {file_name}. Original error: {str(e)}") from e


def extract_scheduled_files(bytes_io, output_dir=None):
    """
    Extract only files with names starting with 'scheduled' from a tar.gz archive.
    
    Args:
        bytes_io: BytesIO object containing the tar.gz file
        output_dir: Optional output directory path. If None, creates a temp directory.
    
    Returns:
        tuple: (local_path, dbfs_path) where files were extracted
    """
    if output_dir is None:
        # Create temp directory with unique name
        tmp_label = uuid.uuid4().hex
        dbfs_path = f"/FileStore/tmp/{tmp_label}"
        local_path = f"/dbfs{dbfs_path}"
    else:
        # Use provided directory
        if output_dir.startswith("/dbfs"):
            local_path = output_dir
            dbfs_path = output_dir[5:]  # Remove "/dbfs" prefix
        else:
            dbfs_path = output_dir
            local_path = f"/dbfs{output_dir}"
    
    # Create directories
    print(f"Creating directories: {local_path} and {dbfs_path}")
    dbutils.fs.mkdirs(dbfs_path)
    os.makedirs(local_path, exist_ok=True)
    
    # Extract scheduled files
    bytes_io.seek(0)
    try:
        extracted_count = 0
        with tarfile.open(fileobj=bytes_io, mode="r:gz") as tar:
            scheduled_files = [n for n in tar.getnames() if n.startswith("scheduled")]
            
            for file_name in scheduled_files:
                file_obj = tar.extractfile(file_name)
                if file_obj:  # Check if file_obj is not None
                    file_path = os.path.join(local_path, file_name)
                    
                    # Create parent directories if needed
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    
                    # Write the file
                    with open(file_path, 'wb') as f:
                        f.write(file_obj.read())
                    extracted_count += 1
        
        print(f"Extracted {extracted_count} scheduled files to {local_path}")
    except Exception as e: 
        print(f"Failed to extract scheduled files: {str(e)}")
        raise
        
    return local_path, dbfs_path


def parse_verification_filename(file_name: str) -> dict:
    """
    Parse dlverreport_ACCOUNTID_YYYYMM.tar.gz
    Returns dict with account_id, year, month, report_yr_month
    Raises ValueError if format is unexpected.
    """
    parts = file_name.replace('.tar.gz', '').split('_')
    if len(parts) < 3:
        raise ValueError(f"Unexpected filename format: {file_name}")
    
    account_id = parts[1]
    if not account_id.isdigit():
        raise ValueError(f"Invalid account ID in filename: {account_id}")
    
    date_part = parts[2]
    if len(date_part) < 6:
        raise ValueError(f"Invalid date in filename: {date_part}")
    
    return {
        "account_id": account_id,
        "year": date_part[:4],
        "month": date_part[4:6],
        "report_yr_month": f"{date_part[:4]}/{date_part[4:6]}"
    }

# COMMAND ----------

# DBTITLE 1,Detail File Processing
def process_detail_file(dir_path, account_id=None):
    """
    Process the scheduled securities detail file from the extracted directory.
    
    Args:
        dir_path: Local path where files are extracted
        account_id: Account ID to use. If None, will try to extract from filename.
    """
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    # Find the scheduled securities detail file
    detail_files = [f for f in os.listdir(dir_path) if f.startswith("scheduled_securities_")]
    
    if not detail_files:
        raise ValueError(f"No scheduled securities detail file found in {dir_path}")
    
    detail_file = os.path.join(dir_path, detail_files[0])
    
    # Validate path exists
    if not os.path.exists(detail_file):
        raise ValueError(f"Detail file not found: {detail_file}")
    
    # Extract account_id from filename if not provided
    if account_id is None:
        try:
            # Assuming filename format: scheduled_securities_ACCOUNTID_YYYYMM
            filename = os.path.basename(detail_file)
            account_id = filename.split('_')[2]
        except IndexError:
            raise ValueError(f"Could not extract account ID from filename: {filename}")
    
    # Validate account_id format
    if not account_id.isdigit():
        raise ValueError(f"Invalid account ID format: {account_id}")
    
    # Convert local path to DBFS path for Spark
    dbfs_path = f"dbfs:{detail_file.replace('/dbfs', '')}"
    
    # Read and process the file
    df = spark.read.csv(dbfs_path, header=True, sep="|", inferSchema=True)
    
    df = df.withColumns({
        "ACCOUNT_ID": F.lit(account_id),
        "PROCESSED_TIME": F.to_timestamp(df["PROCESSED_TIME"], "EEE MMM dd HH:mm:ss zzz yyyy"),
    })

    df = df.groupBy(df.columns).agg(F.count("*").cast("int").alias("REQUEST_COUNT"))
    
    # validation
    expected_cols = {c.upper() for c in sched_detail_table.current().columns}

    row_count = df.count()
    if row_count == 0:
        raise ValueError(f"Empty detail dataframe for {dir_path}")
    print(f"Detail file: {row_count} rows")

    null_accounts = df.where(col("ACCOUNT_ID").isNull()).count()
    if null_accounts > 0:
        raise ValueError(f"Found {null_accounts} rows with null ACCOUNT_ID")

    null_timestamps = df.where(col("PROCESSED_TIME").isNull()).count()
    if null_timestamps > 0:
        print(f"Warning: Found {null_timestamps} rows with null PROCESSED_TIME (timestamp parse failures?)")

    missing = set(expected_cols) - {c.upper() for c in df.columns}
    if missing:
        raise ValueError(f"Detail dataframe missing columns: {missing}")
        
    # Return the dataframe instead of displaying it
    return df

# COMMAND ----------

# DBTITLE 1,Summary File Processing
def process_summary_file(dir_path, account_id=None, report_yr_month=None):
    """
    Process the scheduled securities summary file from the extracted directory.
    
    Args:
        dir_path: Local path where files are extracted
        account_id: Account ID to use. If None, will try to extract from filename.
        report_yr_month: Report year/month in format YYYY/MM. If None, will try to extract from filename.
    """
    # Find the summary file
    summary_files = [f for f in os.listdir(dir_path) if f.startswith("scheduled_summary_")]
    
    if not summary_files:
        raise ValueError(f"No scheduled summary file found in {dir_path}")
    
    summary_file = os.path.join(dir_path, summary_files[0])
    
    # Validate path exists
    if not os.path.exists(summary_file):
        raise ValueError(f"Summary file not found: {summary_file}")
    
    # Extract account_id and report date from filename if not provided
    if account_id is None or report_yr_month is None:
        try:
            # Assuming filename format: scheduled_summary_ACCOUNTID_YYYYMM
            filename = os.path.basename(summary_file)
            parts = filename.split('_')
            
            if account_id is None:
                account_id = parts[2]
            
            if report_yr_month is None and len(parts) > 3:
                # Convert YYYYMM to MM/YYYY format
                date_part = parts[3].replace('.csv', '')
                if len(date_part) >= 6:
                    year = date_part[:4]
                    month = date_part[4:6]
                    report_yr_month = f"{year}/{month}"
        except (IndexError, ValueError) as e:
            raise ValueError(f"Could not extract info from filename: {filename}. Error: {str(e)}")
    
    # Convert local path to DBFS path for Spark
    dbfs_path = f"dbfs:{summary_file.replace('/dbfs', '')}"
    
    # Read and process the file
    summary_df = spark.read.csv(dbfs_path, header=True, sep="|", inferSchema=True)
    
    # Apply transformations
    summary_df = summary_df.withColumns({
        "ACCOUNT_ID": F.lit(account_id),
        "REPORT_YR_MONTH": F.lit(report_yr_month),
        "TOTAL_USAGE": F.when(F.col("TOTAL_USAGE") == "N/A", 0)
                         .otherwise(F.col("TOTAL_USAGE").cast("int")),
        "CLIENT_USAGE": F.when(F.col("CLIENT_USAGE") == "N/A", 0)
                         .otherwise(F.col("CLIENT_USAGE").cast("int")),
        "GROUP_AC": F.when(F.col("GROUP_AC") == "N/A", 0)
                    .otherwise(F.col("GROUP_AC").cast("int")),
        "CLIENT_BO_OPT_AC": F.when(F.col("CLIENT_BO_OPT_AC") == "N/A", 0)
                            .otherwise(F.col("CLIENT_BO_OPT_AC").cast("int")),
        "YR_MONTH_OF_MOST_RECENT_DOWNLOAD":
            F.concat(
                F.substring(col("MONTH_YR_OF_MOST_RECENT_DOWNLOAD"), 4, 4),
                F.lit("/"),
                F.substring(col("MONTH_YR_OF_MOST_RECENT_DOWNLOAD"), 1, 2)
            )
    })
    
    # Select columns to match the target table
    summary_df = summary_df.select([str.upper(c) for c in sched_summary_table.current().columns])

    # validation
    row_count = summary_df.count()
    if row_count == 0:
        raise ValueError(f"Empty summary dataframe for {dir_path}")
    print(f"Summary file: {row_count} rows")

    expected_cols = {c.upper() for c in sched_summary_table.current().columns}
    missing = expected_cols - {c.upper() for c in summary_df.columns}
    if missing:
        raise ValueError(f"Summary dataframe missing columns: {missing}")
    
    return summary_df

# COMMAND ----------

# DBTITLE 1,Main Routine
def process_verification_file(file_name: str, dryrun: bool = False) -> Processing_Status:
    """
    Log-based processing model:
    1. Check if file is already processed (name + checksum in log)
    2. Log PROCESSING status (log is the single source of truth)
    3. Extract scheduled files from tar to tmp storage
    4. Process detail and summary files
    5. Clean up tmp storage
    6. Log COMPLETED status
    7. Move file from INCOMING to ARCHIVE (single move on success only)

    On failure: log FAILED status, file stays in INCOMING for retry.
    """
    tmp_dir_local = None
    tmp_dir_dbfs = None

    file_size = 0
    file_checksum = ""

    try:

        bytes_io = get_file_bytesio(file_name)
        file_checksum = calculate_checksum(bytes_io)
        file_size = get_bytesio_size(bytes_io)

        # Extract account_id from filename if possible
        account_id = None
        try:
            file_info = parse_verification_filename(file_name)
            account_id = file_info["account_id"]
            report_yr_month = file_info["report_yr_month"]
        except ValueError as e:
            print(f"Warning: {e} - will attempt to extract from inner files")
            account_id = None
            report_yr_month = None
        
        print(f"Processing file: {file_name}")
        print(f"Account ID: {account_id}")
        print(f"Report month/year: {report_yr_month}")

        if is_processed(file_name, file_checksum):
            print(f"File already processed: {file_name}")
            return Processing_Status.SKIPPED
        
        if not dryrun:
            # Log PROCESSING status — log is sole state tracker, no folder move
            log_status(file_name, file_size, file_checksum, Processing_Status.PROCESSING)
        
        # Extract the scheduled files from tar to tmp storage
        tmp_dir_local, tmp_dir_dbfs = extract_scheduled_files(bytes_io)
        print(f"Temporary directory: {tmp_dir_local} ({tmp_dir_dbfs})")
        
        # Process detail and summary files
        detail_df = None
        summary_df = None

        try:
            detail_df = process_detail_file(tmp_dir_local, account_id)
        except ValueError:
            print(f"Failed to process detail file in {file_name}")

        try:
            summary_df = process_summary_file(tmp_dir_local, account_id, report_yr_month)
        except ValueError:
            print(f"Failed to process summary file in {file_name}")

        # fail if we extracted nothing
        if detail_df is None and summary_df is None:
            raise ValueError(f"No data extracted from {file_name} - 0 scheduled files in archive")
        
        # If not a dry run, save the data to tables
        if not dryrun:
            # Write to tables
            if detail_df is not None:
                print(f"Writing to sched_detail table")
                sched_detail_table.updateWithColumns(detail_df)
            else:
                print(f"Skipping writing to sched_detail table")

            if summary_df is not None:
                print(f"Writing to sched_summary table")
                sched_summary_table.updateWithColumns(summary_df)
            else: 
                print(f"Skipping writing to sched_summary table")

        else:
            # Just display the data
            if detail_df is not None:
                detail_df.display()
            if summary_df is not None:
                summary_df.display()
        
        if not dryrun:
            # Log COMPLETED, then archive (single move: INCOMING → ARCHIVE)
            log_status(file_name, file_size, file_checksum, Processing_Status.COMPLETED)
            
            dbutils.fs.mv(
                _storage + INCOMING + "/" + file_name,
                _storage + ARCHIVE + "/" + file_name
            )
            return Processing_Status.COMPLETED

    except Exception as e:
        print(f"Error processing file {file_name}: {e}")
        if not dryrun:
            try:
                # Log FAILED — file stays in INCOMING for retry
                log_status(file_name, file_size, file_checksum, Processing_Status.FAILED, str(e))
            except Exception:
                print(f"Failed to update status in the process log")

        return Processing_Status.FAILED
            

    finally:
        # Cleanup
        if tmp_dir_dbfs is not None:
            try:
                dbutils.fs.rm(tmp_dir_dbfs, recurse=True)
                print(f"Cleaned up temporary directory: {tmp_dir_dbfs}")
            except Exception as cleanup_error:
                print(f"Failed to clean up: {cleanup_error}")


# COMMAND ----------

# DBTITLE 1,Batch Loop
files_to_load = dbutils.fs.ls(_storage + INCOMING)
total = len(files_to_load)
results = []

for i, file in enumerate(files_to_load, 1):
  file_info = parse_verification_filename(file.name)
  if file_info["account_id"] not in KNOWN_ACCOUNTS:
    print(f"Skipping file {file.name} - unknown account {file_info['account_id']}")
    dbutils.fs.mv(file.path, _storage + EXCLUDED + "/" + file.name)
    continue
  print(f"Processing file {i}/{total}: {file.name}")
  status = process_verification_file(file.name, dryrun=False)
  results.append({"file": file.name, "status": status})

print(f"\n{'='*40}")
print(f"Batch complete: {total} files")
for s in Processing_Status:
  count = sum(1 for r in results if r["status"] == s)
  if count > 0:
    print(f"  {s.value}: {count}")

# COMMAND ----------

# DBTITLE 1,List Files in Processing
# Show files currently in-flight (PROCESSING in log but not yet COMPLETED/FAILED)
# These are actively being processed right now, or were interrupted mid-run
proc_df = process_log_table.current()
window_spec = Window.partitionBy("file_name").orderBy(proc_df["process_date"].desc())
in_flight_df = (
    proc_df
    .withColumn("row_num", row_number().over(window_spec))
    .where("row_num = 1")
    .drop("row_num")
    .where(col("process_status") == Processing_Status.PROCESSING.value)
    .orderBy(col("process_date").desc())
)
print(f"Files currently in-flight: {in_flight_df.count()}")
in_flight_df.display()

# COMMAND ----------

# DBTITLE 1,Check for any remaining files
# Show files whose latest log status is FAILED — still in INCOMING, eligible for retry
proc_df = process_log_table.current()
window_spec = Window.partitionBy("file_name").orderBy(proc_df["process_date"].desc())
failed_df = (
    proc_df
    .withColumn("row_num", row_number().over(window_spec))
    .where("row_num = 1")
    .drop("row_num")
    .where(col("process_status") == Processing_Status.FAILED.value)
    .orderBy(col("process_date").desc())
)
print(f"Failed files (in INCOMING, ready for retry): {failed_df.count()}")
failed_df.display()

# COMMAND ----------

# DBTITLE 1,List Files By Process Date
process_log_table.current().orderBy(col("file_name"), col("process_date").desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Manual Testing

# COMMAND ----------

# DBTITLE 1,manual test schema
sched_detail_table.current().columns

# COMMAND ----------

sched_detail_table.current().printSchema()