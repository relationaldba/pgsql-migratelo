#!/bin/bash
####################################################################
# Script to migrate PostgreSQL large objects
#
# Arg1 (optional): migrate LOBs starting this OID value (OID_START), default 0 
# Arg2 (optional): migrate LOBs upto this OID value (OID_END), default 4294967295
#
# Example pg-migratelo.sh migrate_lo 5000 30000
# This will find missing OIDs in target between 5000 and 30000 and migrate them
# Migration will be in batches, per the BATCH_SIZE param in the config file
####################################################################

# Read arguments
# Get the action argument
ACTION=$1;

# if [[ -z $1 || $1 == 0 ]]; then MIGRATION_OID_LIST="migration-oids"; else MIGRATION_OID_LIST=$1; fi;
if [[ -z $2 || $2 == 0 ]]; then OID_START=1; else OID_START=$2; fi;
if [[ -z $3 || $3 == 0 ]]; then OID_END=4294967295; else OID_END=$3; fi;

# Declare variables
#############################################
# Source database
SRC_PGHOST=<source_pg_host>
SRC_PGPORT=5432
SRC_PGDATABASE=<source_db>
SRC_PGUSER=<source_pg_user>
SRC_PGPASSWORD=<strong_password>
SRC_PGSSLMODE=prefer

#############################################
# Target database
TGT_PGHOST=<target_pg_host>
TGT_PGPORT=5432
TGT_PGDATABASE=<target_db>
TGT_PGUSER=<target_pg_user>
TGT_PGPASSWORD=<strong_password>
TGT_PGSSLMODE=prefer

#############################################
# Batch size and jobs
BATCH_SIZE=10000;
LOB_JOBS=8;
FOLLOW_LO="true";
VERIFY_AFTER_MIGRATE="true";
MISSING_OIDS_FILE="missing_oids.txt"


SRC_CONNSTRING="postgresql:///${SRC_PGDATABASE}?host=${SRC_PGHOST}&port=${SRC_PGPORT}&user=${SRC_PGUSER}&password=${SRC_PGPASSWORD}&sslmode=${SRC_PGSSLMODE}";
TGT_CONNSTRING="postgresql:///${TGT_PGDATABASE}?host=${TGT_PGHOST}&port=${TGT_PGPORT}&user=${TGT_PGUSER}&password=${TGT_PGPASSWORD}&sslmode=${TGT_PGSSLMODE}";

# Start recording the elapsed seconds using the bash's internal counter
SECONDS=0;

# Variable EXIT_CODE stores the outcome, 0 for success, else failure
EXIT_CODE=0;

# Declare functions
function err() {
  echo "$(date +'%Y-%m-%d %H:%M:%S') ERROR [${ACTION}] $*" >&2;
}

function log() {
  echo "$(date +'%Y-%m-%d %H:%M:%S') INFO  [${ACTION}] $*" >&1;
}

function end() {
    log "Completed in $(( $SECONDS / 3600 ))h $(( $SECONDS % 3600 / 60 ))m $(( $SECONDS % 60 ))s";
    rm -rf "tmp";
    exit $1;
}

function validate_config() {

    local return_code=0;

    # Verify if the PostgreSQL connection params are specified in the env variables/file
    if [[ -z $SRC_PGHOST || -z $SRC_PGUSER || -z $SRC_PGPASSWORD || -z $SRC_PGDATABASE \
        || -z $TGT_PGHOST || -z $TGT_PGUSER || -z $TGT_PGPASSWORD || -z $TGT_PGDATABASE ]]
    then
        err "Connection parameter(s) invalid or missing"
        err "Verify the environment variables SRC_PGHOST, SRC_PGUSER, SRC_PGPASSWORD and SRC_PGDATABASE";
        err "Verify the environment variables TGT_PGHOST, TGT_PGUSER, TGT_PGPASSWORD and TGT_PGDATABASE";
        return_code=1;
        return $return_code;
    fi

    # If PGPORT is not specified, assume 5432
    [[ $SRC_PGPORT == "" ]] && SRC_PGPORT=5432;
    [[ $TGT_PGPORT == "" ]] && TGT_PGPORT=5432;

    # If the requested action does not match the expected value, exit with an error
    if [[ ${ACTION} != "migrate_lo" ]] \
        && [[ ${ACTION} != "verify_lo" ]]
    then
        err "Invalid/missing argument";
        err "Usage:";
        err "pgsql-migratelo.sh migrate_lo                      # migrate all LOBs from src to tgt     #";
        err "pgsql-migratelo.sh migrate_lo oid_start oid_end    # migrate LOBs between the range       #";
        err "pgsql-migratelo.sh verify_lo                       # verify if the LOBs in src match tgt  #";
        err "pgsql-migratelo.sh verify_lo  oid_start oid_end    # verify LOBs between the range        #";
        return_code=1;
        return $return_code;
    fi

    # Confirm oid start is less than oid end
    (( $OID_START > $OID_END )) \
        && err "Invalid arguments, oid start must be less than oid end" \
        && return_code=1 \
        && return $return_code;

    return $return_code;
}

function migrate_lo() {

    local oid_start=$1;
    local oid_end=$2;
    local return_code=0;
    local lo_file="tmp/${oid_start}_${oid_end}"

    if [[ $oid_start == "" || $oid_start == 0 || $oid_end == "" || $oid_end == 0 ]]
    then
        err "Invalid start/end range for LOB oids";
        return_code=1;
        return $return_code;
    fi

    # Get the batch of large object OIDs from source store the batch locally on the disk
    psql_result=$(psql $SRC_CONNSTRING \
        --csv \
        --field-separator="," \
        --tuples-only \
        --no-align \
        --quiet \
        --output="${lo_file}" \
        --command="SELECT pg_largeobject_metadata.oid, lo_get(pg_largeobject_metadata.oid)::text 
            FROM pg_largeobject_metadata 
            WHERE oid >= ${oid_start}
            AND oid < ${oid_end}
            ORDER BY oid;" 2>&1);
    
    (( $? != 0 )) \
        && err "Failed to export LOBs between range oid: ${oid_start} and oid: ${oid_end}" \
        && err "${psql_result}" \
        && return_code=1 \
        && rm -f "${lo_file}" \
        && return $return_code;

    # If the fle is not empty, import the LOBs into the target database
    if [[ -s "${lo_file}" ]]
    then

        # log "Migrating LOBs from oid ${oid_start} to ${oid_end}"

        psql_result=$(psql $TGT_CONNSTRING \
            --quiet \
            --command "CREATE TEMP TABLE temp_largeobject (loid bigint, data bytea);" \
            --command "\COPY temp_largeobject FROM ${lo_file} WITH DELIMITER ',' CSV;" \
            --command "SELECT pg_create_lo_from_bytea(loid, data) FROM temp_largeobject;" 2>&1);
    
        (( $? != 0 )) \
            && err "Failed to import LOBs between range oid: ${oid_start} and oid: ${oid_end}" \
            && err "${psql_result}" \
            && return_code=1 \
            && rm -f "${lo_file}" \
            && return $return_code;

        log "Migrated LOBs from oid ${oid_start} to ${oid_end}"
    fi

    rm -f "${lo_file}";
    return $return_code;
}

function verify_lo() {

    local oid_start=$1;
    local oid_end=$2;
    local return_code=0;
    local lo_file_src="tmp/${oid_start}_${oid_end}_src";
    local lo_file_tgt="tmp/${oid_start}_${oid_end}_tgt";
    local missing_oids=$MISSING_OIDS_FILE

    # Get the batch of large object OIDs from source and target, store the batch locally on the disk
    # Its important to sort the OID list as text to allow the comm utility to perform a quick compare 
    # and list the values that are unique to source
    psql_result=$(psql $SRC_CONNSTRING \
        --csv --field-separator=" " \
        --tuples-only --no-align \
        --quiet \
        --output="$lo_file_src" \
        --command="SELECT oid, lo_get(oid)::text 
            FROM pg_largeobject_metadata 
            WHERE oid >= ${oid_start} 
            AND oid < ${oid_end} 
            ORDER BY oid::text;" 2>&1);
    
    (( $? != 0 )) \
        && err "Failed to export OIDs from source for range ${oid_start} to ${oid_end}" \
        && err "${psql_result}" \
        && return_code=1 \
        && rm -f "${lo_file_src}" \
        && return $return_code;

    psql_result=$(psql $TGT_CONNSTRING \
        --csv --field-separator=" " \
        --tuples-only --no-align \
        --quiet \
        --output="$lo_file_tgt"  \
        --command="SELECT oid, lo_get(oid)::text 
            FROM pg_largeobject_metadata 
            WHERE oid >= ${oid_start} 
            AND oid < ${oid_end} 
            ORDER BY oid::text;" 2>&1);
    
    (( $? != 0 )) \
        && err "Failed to export OIDs from source for range ${oid_start} to ${oid_end}" \
        && err "${psql_result}" \
        && return_code=1 \
        && rm -f "${lo_file_tgt}" \
        && return $return_code;

    # Compare the source and target OIDs and store the diff locally
    set -o pipefail
    comm -2 -3 "${lo_file_src}" "${lo_file_tgt}" | cut -d' ' -f1  1>>"${missing_oids}";
    
    (( $? != 0 )) \
        && err "Failed to compare OIDs in files ${lo_file_src} and ${lo_file_tgt}" \
        && return_code=1 \
        && rm -f "${lo_file_src}" \
        && rm -f "${lo_file_tgt}" \
        && return $return_code;

    
    rm -f "${lo_file_src}"
    rm -f "${lo_file_tgt}"
    return $return_code;
}

function create_lo_from_bytea_function() {

    local return_code=0;
    local tgt_connstring=$1;
    
    local pg_lob_function="
        CREATE OR REPLACE FUNCTION pg_create_lo_from_bytea(lobOid bigint, lobData bytea)
        RETURNS oid AS \$\$
        DECLARE 
            loid oid;
        BEGIN

            PERFORM oid FROM pg_largeobject_metadata WHERE oid = lobOid;

            IF FOUND THEN
                PERFORM lo_unlink(lobOid);
            END IF;

            loid := lo_from_bytea(lobOid, lobData);

            RETURN loid;

        END;
        \$\$ LANGUAGE plpgsql;"

    # Create the function needed to restore the large objects from bytea
    psql_result=$(psql $tgt_connstring --quiet --command="$pg_lob_function" 2>&1);
    return_code=$?;
    
    (( $return_code != 0 )) \
        && err "Failed to create the function pg_create_lo_from_bytea" \
        && err "${psql_result}";
        # || log "Created the function pg_create_lo_from_bytea";

    return $return_code;
}


# log "Src: ${SRC_CONNSTRING}";
# log "Tgt: ${TGT_CONNSTRING}";

# Validate the configuration 
validate_config;
(( $? != 0 )) && end $?;

# Create a temp folder to store the temp files
mkdir -p "tmp";
(( $? != 0 )) && end $?;

# Log connection info
log "Source server: ${SRC_PGHOST}";
log "Source database: ${SRC_PGDATABASE}";
log "Target server: ${TGT_PGHOST}";
log "Target database: ${TGT_PGDATABASE}";
log "Parallel jobs: ${LOB_JOBS}";
log "Batch size per job: ${BATCH_SIZE}";

# Get the min and max OID value from source and target
SRC_MINOID=0;
SRC_MINOID=$(psql $SRC_CONNSTRING --tuples-only --no-align --command="SELECT min(oid) FROM pg_largeobject_metadata;");
(( $? != 0 )) \
    && err "Unable to get the min OID value from source" \
    && end 1;

SRC_MAXOID=0;
SRC_MAXOID=$(psql $SRC_CONNSTRING --tuples-only --no-align --command="SELECT max(oid) FROM pg_largeobject_metadata;");
(( $? != 0 )) \
    && err "Unable to get the max OID value from source" \
    && end 1;

# Set the floor to greatest of SRC_MINOID and OID_START
if (( $OID_START < $SRC_MINOID ))
then
    OID_START=$SRC_MINOID;
fi

# Set the ceiling to least of SRC_MAXOID and OID_END
if (( $OID_END > $SRC_MAXOID ))
then
    OID_END=$SRC_MAXOID;
fi

log "OID start: ${OID_START}";
log "OID end: ${OID_END}";




if [[ ${ACTION} == "migrate_lo" ]]
then

    # Create the LOB from bytea function in 
    create_lo_from_bytea_function $TGT_CONNSTRING;
    (( $? != 0 )) \
        && end $? \
        || log "Created the function pg_create_lo_from_bytea";

    # Start the actual migration
    log "Migrating LOBs from source to target";
    OID_OFFSET_START=$OID_START;
    OID_OFFSET_END=$(( OID_START + BATCH_SIZE ));
    BGPIDS=();

    while true
    do
        # sleep 0.1
        # log "BGPIDS ${#BGPIDS[@]}, LOB_JOBS $LOB_JOBS, OID_OFFSET_START $OID_OFFSET_START, OID_OFFSET_END $OID_OFFSET_END"

        if (( ${#BGPIDS[@]} < $LOB_JOBS )) && (( $OID_OFFSET_START <= $OID_END ))
        then
            migrate_lo $OID_OFFSET_START $OID_OFFSET_END &

            pid="$!"
            BGPIDS+=("${pid}")

            # log "Blob migration subprocess ${pid} exporting from ${OID_OFFSET_START} to ${OID_OFFSET_END}"
            
            # Increment the offset for next batch
            OID_OFFSET_START=$(( OID_OFFSET_START + BATCH_SIZE ))
            OID_OFFSET_END=$(( OID_OFFSET_END + BATCH_SIZE ))
        
        elif (( ${#BGPIDS[@]} == 0 )) && (( $OID_OFFSET_START > $OID_END ))
        then
            break;
        fi


        # Check if the background PIDs completed execution, if yes remove them from the array BGPIDS
        for i in ${!BGPIDS[@]}
        do

            p=${BGPIDS[i]}
            # Check if the folder /proc/pid exists. 
            # If exists then the pid still running, if no then pid is finished execution
            # Check the exit code of the finished pid and remove the pid from the BGPIDS array
            if [[ ! -d "/proc/${p}" ]]
            then
                if ! wait $p
                then
                    # log "Blob migration subprocess ${p} succeeded";
                    err "Blob migration subprocess ${p} failed";
                    EXIT_CODE=1;
                    break 2;
                fi

                # remove the pid from the BGPIDS array
                unset BGPIDS[i]
            fi
        done

    done

    (( $EXIT_CODE != 0 )) \
        && err "Failed to migrate LOBs for OID range $OID_START to $OID_END, see logs above" \
        || log "Successfully migrated LOBs for OID range $OID_START to $OID_END"; 

    (( $EXIT_CODE != 0 )) && end $EXIT_CODE;

fi


if [[ ${ACTION} == "verify_lo" ]] || ${VERIFY_AFTER_MIGRATE} == "true" ]]
then
    
    # Get the total count of LOBs from source and target
    SRC_LOBCOUNT=$(psql $SRC_CONNSTRING --tuples-only --no-align --command="SELECT count(oid) FROM pg_largeobject_metadata;")
    (( $? != 0 )) \
        && err "Unable to get the count of LOBs from source" \
        && end 1;

    TGT_LOBCOUNT=$(psql $TGT_CONNSTRING --tuples-only --no-align --command="SELECT count(oid) FROM pg_largeobject_metadata;")
    (( $? != 0 )) \
        && err "Unable to get the count of LOBs from target" \
        && end 1;

    # Get the max OID value from source and target
    SRC_MAXOID=$(psql $SRC_CONNSTRING --tuples-only --no-align --command="SELECT max(oid) FROM pg_largeobject_metadata;");
    (( $? != 0 )) \
        && err "Unable to get the max OID value from source" \
        && end 1;

    TGT_MAXOID=$(psql $TGT_CONNSTRING --tuples-only --no-align --command="SELECT max(oid) FROM pg_largeobject_metadata;");
    (( $? != 0 )) \
        && err "Unable to get the max OID value from target" \
        && end 1;

    # Compare source and target counts
    (( $SRC_LOBCOUNT == $TGT_LOBCOUNT )) \
        && log "The LOB count in source ${SRC_LOBCOUNT} matches with target ${TGT_LOBCOUNT}" \
        || err "The LOB count in source ${SRC_LOBCOUNT} does not match with target ${TGT_LOBCOUNT}";
    (( $SRC_MAXOID == $TGT_MAXOID )) \
        && log "The max OID value in source ${SRC_MAXOID} matches with target ${TGT_MAXOID}" \
        || err "The max OID value in source ${SRC_MAXOID} does not match with target ${TGT_MAXOID}";

    # # Set the ceiling of offset to the least of SRC_MAXOID and OID_END
    # if [[ $OID_END -gt $SRC_MAXOID ]]; then
    #     OID_END=$SRC_MAXOID;
    # fi

    # Log additional data
    log "Source LOB count: ${SRC_LOBCOUNT}";
    log "Source max OID: ${SRC_MAXOID}";
    log "Target LOB count: ${TGT_LOBCOUNT}";
    log "Target max OID: ${TGT_MAXOID}";
    log "Batch start: ${OID_START}";
    log "Batch end: ${OID_END}";


    # Start the actual verification
    # At the end of verification, prepare a list of 
    #   - Missing OIDs - LOBs present in source but missing in target
    #   - Mismatch OIDs - LOBs present in target, but data does not match with source
    # The list of OIDs is stored in a text file, which can be passed to the migration script for LOB migration

    log "Verifying missing/mismatch LOB OIDs";
    OID_OFFSET_START=$OID_START;
    OID_OFFSET_END=$(( OID_START + BATCH_SIZE ));
    BGPIDS=();

    while true
    do
        # sleep 0.1
        # log "BGPIDS ${#BGPIDS[@]}, LOB_JOBS $LOB_JOBS, OID_OFFSET_START $OID_OFFSET_START, OID_OFFSET_END $OID_OFFSET_END"

        if (( ${#BGPIDS[@]} < $LOB_JOBS )) && (( $OID_OFFSET_START <= $OID_END ))
        then
            verify_lo $OID_OFFSET_START $OID_OFFSET_END &

            pid="$!"
            BGPIDS+=("${pid}")

            log "Blob verification subprocess ${pid} verifying from ${OID_OFFSET_START} to ${OID_OFFSET_END}"
            
            # Increment the offset for next batch
            OID_OFFSET_START=$(( OID_OFFSET_START + BATCH_SIZE ))
            OID_OFFSET_END=$(( OID_OFFSET_END + BATCH_SIZE ))
        
        elif (( ${#BGPIDS[@]} == 0 )) && (( $OID_OFFSET_START > $OID_END ))
        then
            break;
        fi


        # Check if the background PIDs completed execution, if yes remove them from the array BGPIDS
        for i in ${!BGPIDS[@]}
        do

            p=${BGPIDS[i]}
            # Check if the folder /proc/pid exists. 
            # If exists then the pid still running, if no then pid is finished execution
            # Check the exit code of the finished pid and remove the pid from the BGPIDS array
            if [[ ! -d "/proc/${p}" ]]
            then
                if ! wait $p
                then
                    # log "Blob migration subprocess ${p} succeeded";
                    err "Blob verification subprocess ${p} failed";
                    EXIT_CODE=1;
                    break 2;
                fi

                # remove the pid from the BGPIDS array
                unset BGPIDS[i]
            fi
        done

    done

    (( $EXIT_CODE != 0 )) \
        && err "Failed to verify LOBs for OID range ${OID_START} to ${OID_END}, see logs above" \
        || log "Successfully verified LOBs for OID range ${OID_START} to ${OID_END}"; 

    MISSING_LOB_COUNT=$(wc -l "$MISSING_OIDS_FILE" | cut -d' ' -f1)
    
    (( $MISSING_LOB_COUNT == 0 )) \
        && log "LOBs to migrate: ${MISSING_LOB_COUNT}, migration not required" \
        || log "LOBs to migrate: ${MISSING_LOB_COUNT}, please review ${MISSING_OIDS_FILE}";
    
    [[ ${ACTION} == "verify_lo" ]] \
        && end $EXIT_CODE;

fi


# If FOLLOW_LO is true, then perform continuous migration of LOBs
if [[ ${ACTION} == "migrate_lo" && ${FOLLOW_LO} == "true" ]]
then

    log "Following LOBs from source";

    while true
    do

        sleep 60;

        TGT_MAXOID=$(psql $TGT_CONNSTRING --tuples-only --no-align --command="SELECT max(oid) FROM pg_largeobject_metadata;");
        (( $? != 0 )) \
            && err "Unable to get the max OID value from target" \
            && end 1;
        
        OID_OFFSET_START=$(( TGT_MAXOID + 1 ));
        OID_OFFSET_END=$(( OID_OFFSET_START + BATCH_SIZE ));
    
        # log "BGPIDS ${#BGPIDS[@]}, LOB_JOBS $LOB_JOBS, OID_OFFSET_START $OID_OFFSET_START, OID_OFFSET_END $OID_OFFSET_END"

        # log "Migrating LOBs from oid ${OID_OFFSET_START}";
        
        migrate_lo $OID_OFFSET_START $OID_OFFSET_END;
        (( $? != 0 )) \
            && err "Blob follow failed" \
            && end 1;

    done
fi


end $EXIT_CODE;
