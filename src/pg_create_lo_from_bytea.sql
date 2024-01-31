CREATE OR REPLACE FUNCTION pg_create_lo_from_bytea(lobOid int8, lobData bytea)
RETURNS oid AS $$
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
$$ LANGUAGE plpgsql;