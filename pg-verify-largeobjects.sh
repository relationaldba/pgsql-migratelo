#!/bin/bash
####################################################################
# Script to verify PostgreSQL large objects after migration
#
# Arg1 (optional): verify LOBs starting this OID value (OID_OFFSET_START), default 0 
# Arg2 (optional): verify LOBs upto this OID value (OID_OFFSET_END), default 4294967295
#
# Example ./pg-verify-blob.sh 5000 30000
# This will verify 25000 LOBs, starting OFFSET 5000 upto OFFSET 30000
# Verification will be in batches, as per the value set for the LIMIT param in the config file
####################################################################

# Read arguments
if [[ -z $1 || $1 == 0 ]]; then OID_OFFSET_START=0; else OID_OFFSET_START=$1; fi;
if [[ -z $2 || $2 == 0 ]]; then OID_OFFSET_END=4294967295; else OID_OFFSET_END=$2; fi;


# Start recording the elapsed seconds using the bash inernal counter
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


# Declare variables
SRC_LOBCOUNT=0;
TGT_LOBCOUNT=0;
SRC_MAXOID=0;
TGT_MAXOID=0;
OID_OFFSET=0;
MISSING_LOB_COUNT=0;
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


# Compare source and target counts
[[ $SRC_LOBCOUNT == $TGT_LOBCOUNT ]] && log "The LOB count in source ${SRC_LOBCOUNT} matches with target ${TGT_LOBCOUNT}" \
    || err "The LOB count in source ${SRC_LOBCOUNT} does not match with target ${TGT_LOBCOUNT}";
[[ $SRC_MAXOID == $TGT_MAXOID ]] && log "The max OID value in source ${SRC_MAXOID} matches with target ${TGT_MAXOID}" \
    || err "The max OID value in source ${SRC_MAXOID} does not match with target ${TGT_MAXOID}";


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


# Start the actual verification
# At the end of verification, prepare a list of 
#   - Missing OIDs - LOBs present in source but missing in target
#   - Mismatch OIDs - LOBs present in target, but data does not match with source
# The list of OIDs is stored in a text file, which can be passed to the migration script for LOB migration

log "Finding missing/mismatch LOB OIDs";
OID_OFFSET=$OID_OFFSET_START;
while [[ $OID_OFFSET -le $OID_OFFSET_END ]]
do

    # Get the batch of large object OIDs from source and target, store the batch locally on the disk
    # Its important to sort the OID list as text to allow the comm utility to perform a quick compare 
    # and list the values that are unique to source
    psql $SRC_CONNSTRING --csv --field-separator=" " --tuples-only --no-align --quiet --output="temp-${$}/src-oids"  \
        --command="SELECT oid, lo_get(oid)::text FROM pg_largeobject_metadata WHERE oid >= ${OID_OFFSET} AND oid < $(( $OID_OFFSET + $BATCH_SIZE )) ORDER BY oid::text;"
    [[ $? != 0 ]] && err "Failed to export OIDs from source into csv for range ${OID_OFFSET} to $(( $OID_OFFSET + $BATCH_SIZE ))" && end 1;

    psql $TGT_CONNSTRING --csv --field-separator=" " --tuples-only --no-align --quiet --output="temp-${$}/tgt-oids"  \
        --command="SELECT oid, lo_get(oid)::text FROM pg_largeobject_metadata WHERE oid >= ${OID_OFFSET} AND oid < $(( $OID_OFFSET + $BATCH_SIZE )) ORDER BY oid::text;"
    [[ $? != 0 ]] && err "Failed to export OIDs from target into csv for range ${OID_OFFSET} to $(( $OID_OFFSET + $BATCH_SIZE ))" && end 1;

    # Compare the source and target OIDs and store the diff locally
    comm -2 -3 "temp-${$}/src-oids" "temp-${$}/tgt-oids" |  cut -d' ' -f1  1>>"${$}-missing-oids";
    [[ $? != 0 ]] && err "Failed to compare OIDs in files temp-${$}/src-oids temp-${$}/tgt-oids" && end 1;

    log "Compared OIDs for range ${OID_OFFSET} to $(( $OID_OFFSET + $BATCH_SIZE ))";
    OID_OFFSET=$(( $OID_OFFSET + $BATCH_SIZE ));
done
[[ $? != 0 ]] && err "Failed to generate list of missing/mismatch LOB OIDs" && end 1;


# Count of OIDs to migrate
MISSING_LOB_COUNT=$(wc -l ${$}-missing-oids | cut -d' ' -f1)
[[ $MISSING_LOB_COUNT -eq 0 ]] && log "LOBs to migrate: ${MISSING_LOB_COUNT}, migration not required" && end 0 \
    || log "LOBs to migrate: ${MISSING_LOB_COUNT}, please run the migration script";


# Print success and exit
log "Successfully verified large objects for OID range $OID_OFFSET_START to $OID_OFFSET_END";
end 0;