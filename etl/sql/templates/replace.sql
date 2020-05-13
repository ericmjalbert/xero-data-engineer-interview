DROP TABLE IF EXISTS public._staging_{{ table_name }};

CREATE TABLE public._staging_{{ table_name }} AS 
-- QUERY START --
{{ sql }}
-- QUERY END --
;

BEGIN;

ALTER TABLE IF EXISTS public.{{ table_name }} RENAME TO _backup_{{ table_name }};
ALTER TABLE IF EXISTS public._staging_{{ table_name }} RENAME TO {{ table_name }};
DROP TABLE IF EXISTS public._backup_{{ table_name }};

COMMIT;
