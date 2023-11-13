#!/bin/bash
####################################################################
# Script to migrate PostgreSQL large objects
#
# Arg1 (optional): migrate LOBs starting this OID value (OID_OFFSET_START), default 0 
# Arg2 (optional): migrate LOBs upto this OID value (OID_OFFSET_END), default 4294967295
#
# Example ./pg-migrate-blob.sh 5000 30000
# This will find missing OIDs in target between 5000 and 30000 and migrate them
# Migration will be in batches, per the BATCH_SIZE param in the config file
####################################################################

# Read arguments
if [[ -z $1 || $1 == 0 ]]; then MIGRATION_OID_LIST="migration-oids"; else MIGRATION_OID_LIST=$1; fi;
if [[ -z $2 || $2 == 0 ]]; then OID_OFFSET_START=0; else OID_OFFSET_START=$2; fi;
if [[ -z $3 || $3 == 0 ]]; then OID_OFFSET_END=4294967295; else OID_OFFSET_END=$3; fi;


# Start recording the elapsed seconds using the bash's internal counter
SECONDS=0;


# Declare functions
err() {
  echo "$(date +'%Y-%m-%d %H:%M:%S%z') ERROR $*" >&2;
}

log() {
  echo "$(date +'%Y-%m-%d %H:%M:%S%z') INFO $*" >&1;
}

end() {
    log "The operation completed in $(( $SECONDS / 3600 ))h $(( $SECONDS % 3600 / 60 ))m $(( $SECONDS % 60 ))s";
    rm -rf "temp-${$}";
    exit $1;
}


# Create a temp folder to store the temp files
# Use the current PID in the folder name, so each instance of the script gets its own temp folder 
mkdir -p "temp-${$}";


# Read the config file 
CONFIG_FILE="config";
log "Reading the configuration file ./${CONFIG_FILE}";
source "./${CONFIG_FILE}";
[[ $? != 0 ]] && err "Unable to get read the config file ./${CONFIG_FILE}" && end 1;


# Verify if the migration OID list file exists
if [[ ! -f "${MIGRATION_OID_LIST}" ]]; then
    err "Unable to locate the migration OID list file ./${MIGRATION_OID_LIST}" && end 1;
fi;

# Declare variables
SRC_LOBCOUNT=0;
TGT_LOBCOUNT=0;
SRC_MAXOID=0;
TGT_MAXOID=0;
OID_OFFSET=0;
MIGRATION_LOBCOUNT=0;
SRC_CONNSTRING="postgresql:///${SRC_PGDATABASE}?host=${SRC_PGHOST}&port=${SRC_PGPORT}&user=${SRC_PGUSER}&password=${SRC_PGPASSWORD}&sslmode=${SRC_PGSSLMODE}";
TGT_CONNSTRING="postgresql:///${TGT_PGDATABASE}?host=${TGT_PGHOST}&port=${TGT_PGPORT}&user=${TGT_PGUSER}&password=${TGT_PGPASSWORD}&sslmode=${TGT_PGSSLMODE}";


# Confirm offset start is less than offset end
[[ $OID_OFFSET_START -gt $OID_OFFSET_END ]] && err "Invalid arguments, offset start must be less than offset end" && end 1;


# Log connection info
log "Source server: ${SRC_PGHOST}";
log "Source database: ${SRC_PGDATABASE}";
log "Target server: ${TGT_PGHOST}";
log "Target database: ${TGT_PGDATABASE}";
log "Batch size: ${BATCH_SIZE}";


# Get the total count of LOBs from source and target
SRC_LOBCOUNT=$(psql $SRC_CONNSTRING --tuples-only --no-align --command="SELECT count(oid) FROM pg_largeobject_metadata;")
[[ $? != 0 ]] && err "Unable to get the count of LOBs from source" && end 1;

TGT_LOBCOUNT=$(psql $TGT_CONNSTRING --tuples-only --no-align --command="SELECT count(oid) FROM pg_largeobject_metadata;")
[[ $? != 0 ]] && err "Unable to get the count of LOBs from target" && end 1;


# Get the max OID value from source and target
SRC_MAXOID=$(psql $SRC_CONNSTRING --tuples-only --no-align --command="SELECT max(oid) FROM pg_largeobject_metadata;");
[[ $? != 0 ]] && err "Unable to get the max OID value from source" && end 1;

TGT_MAXOID=$(psql $TGT_CONNSTRING --tuples-only --no-align --command="SELECT max(oid) FROM pg_largeobject_metadata;");
[[ $? != 0 ]] && err "Unable to get the max OID value from target" && end 1;


# Set the ceiling of offset to the least of SRC_MAXOID and OID_OFFSET_END
if [[ $OID_OFFSET_END -gt $SRC_MAXOID ]]; then
    OID_OFFSET_END=$SRC_MAXOID;
fi


# Log additional data
log "Source LOB count: ${SRC_LOBCOUNT}";
log "Source max OID: ${SRC_MAXOID}";
log "Target LOB count: ${TGT_LOBCOUNT}";
log "Target max OID: ${TGT_MAXOID}";
log "Batch start: ${OID_OFFSET_START}";
log "Batch end: ${OID_OFFSET_END}";


# Get the list of missing LOB OIDs in target by comparing source to target 
# log "Generating the list of LOBs to migrate";
# OID_OFFSET=$OID_OFFSET_START;
# while [[ $OID_OFFSET -le $OID_OFFSET_END ]]
# do

#     # Get the batch of large object OIDs from source and target, store the batch locally on the disk
#     # Its important to sort the OID list as text to allow the comm utility to perform a quick compare 
#     # and list the values that are unique to source
#     psql $SRC_CONNSTRING --csv --field-separator="," --tuples-only --no-align --quiet --output="temp-${$}/src-oids"  \
#         --command="SELECT oid FROM pg_largeobject_metadata WHERE oid >= ${OID_OFFSET} AND oid < $(( $OID_OFFSET + $BATCH_SIZE )) ORDER BY oid::text;"
#     [[ $? != 0 ]] && err "Failed to export OIDs from source into csv for range ${OID_OFFSET} to $(( $OID_OFFSET + $BATCH_SIZE ))" && end 1;

#     psql $TGT_CONNSTRING --csv --field-separator="," --tuples-only --no-align --quiet --output="temp-${$}/tgt-oids"  \
#         --command="SELECT oid FROM pg_largeobject_metadata WHERE oid >= ${OID_OFFSET} AND oid < $(( $OID_OFFSET + $BATCH_SIZE )) ORDER BY oid::text;"
#     [[ $? != 0 ]] && err "Failed to export OIDs from target into csv for range ${OID_OFFSET} to $(( $OID_OFFSET + $BATCH_SIZE ))" && end 1;

#     # Compare the source and target OIDs and store the diff locally
#     comm -2 -3 "temp-${$}/src-oids" "temp-${$}/tgt-oids" 1>>"temp-${$}/missing-oids";
#     [[ $? != 0 ]] && err "Failed to compare OIDs in files "temp-${$}"/src-oids "temp-${$}"/tgt-oids" && end 1;

#     log "Compared OIDs for range ${OID_OFFSET} to $(( $OID_OFFSET + $BATCH_SIZE ))";
#     OID_OFFSET=$(( $OID_OFFSET + $BATCH_SIZE ));
# done
# [[ $? != 0 ]] && err "Failed to generate list of missing OIDs" && end 1;
# log "Successfully generated list of missing OIDs";


# Count of OIDs to migrate
MIGRATION_LOBCOUNT=$(wc -l "${MIGRATION_OID_LIST}" | cut -d' ' -f1)
[[ $MIGRATION_LOBCOUNT -eq 0 ]] && log "LOBs to migrate: ${MIGRATION_LOBCOUNT}, migration not required" && end 0 \
    || log "LOBs to migrate: ${MIGRATION_LOBCOUNT}";


# Import the OID list in a temp table in the src database
psql $SRC_CONNSTRING --quiet \
    --command "DROP TABLE IF EXISTS temp_lob_migration;" \
    --command "CREATE TABLE temp_lob_migration (lobid oid PRIMARY KEY);" \
    --command "\COPY temp_lob_migration FROM ${MIGRATION_OID_LIST} WITH DELIMITER ',' CSV;"
[[ $? != 0 ]] && err "Failed to import the list of missing LOB OIDs in source" && end 1;
log "Imported the list of missing LOB OIDs in source";


# Start the actual migration
log "Migrating LOBs from source to target";
OID_OFFSET=0;
while [[ $OID_OFFSET -le $MIGRATION_LOBCOUNT ]]
do

    # Get the batch of large object OIDs from source store the batch locally on the disk
    psql $SRC_CONNSTRING --csv --field-separator="," --tuples-only --no-align --quiet --output="temp-${$}/src-lobdata"  \
        --command="SELECT pg_largeobject_metadata.oid, lo_get(pg_largeobject_metadata.oid)::text 
            FROM pg_largeobject_metadata 
            JOIN temp_lob_migration ON pg_largeobject_metadata.oid = temp_lob_migration.lobid
            ORDER BY oid
            OFFSET ${OID_OFFSET} 
            LIMIT ${BATCH_SIZE};"
    [[ $? != 0 ]] && err "Failed to export LOB data from source into csv for OFFSET ${OID_OFFSET} LIMIT ${BATCH_SIZE}" && end 1;

    # Import the LOBs into the target database
    psql $TGT_CONNSTRING --quiet \
        --command "CREATE TEMP TABLE temp_largeobject (loid int4, data bytea);" \
        --command "\COPY temp_largeobject FROM temp-${$}/src-lobdata WITH DELIMITER ',' CSV;" \
        --command "SELECT pg_create_lo_from_bytea(loid, data) FROM temp_largeobject;" 1>/dev/null
    [[ $? != 0 ]] && err "Failed to import the LOB data into target for OFFSET ${OID_OFFSET} LIMIT ${BATCH_SIZE}" && end 1;

    log "Migrated LOBs for OFFSET ${OID_OFFSET} LIMIT ${BATCH_SIZE}";
    OID_OFFSET=$(( $OID_OFFSET + $BATCH_SIZE ));
done


# Print success and exit
log "Successfully migrated ${MIGRATION_LOBCOUNT} LOBs for OID range $OID_OFFSET_START to $OID_OFFSET_END";
end 0;