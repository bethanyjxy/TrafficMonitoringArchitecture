DO $$ 
DECLARE
    r RECORD;
BEGIN
    -- Loop through each table in the public schema and drop it, but skip 'spatial_ref_sys'
    FOR r IN (SELECT tablename 
              FROM pg_tables 
              WHERE schemaname = 'public' 
              AND tablename != 'spatial_ref_sys') 
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;
