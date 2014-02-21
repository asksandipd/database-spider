INSERT INTO zoo.object_dependencies
SELECT DISTINCT
       referrer_namespace           as dependent_schema
     , referrer_relation            as dependent_table     
     , referrer_attribute           as dependent_column
     , referenced_namespace         as referenced_schema
     , referenced_relation          as referenced_table
     , referenced_attribute         as referenced_column
     , manual		            as manual
  FROM ( SELECT c.oid
              , n.nspname AS constraint_namespace
              , c.conname AS constraint_name
              , s.nspname AS referrer_namespace
              , t.relname AS referrer_relation
              , a.attname AS referrer_attribute 
           FROM pg_constraint c
           JOIN pg_namespace  n ON n.oid = c.connamespace
           JOIN pg_class      t ON t.oid = c.conrelid
           JOIN pg_namespace  s ON s.oid = t.relnamespace
           JOIN pg_attribute  a ON a.attrelid = c.conrelid
                               AND a.attnum = c.conkey[1]
          WHERE c.contype = 'f'
       ) referer
  LEFT JOIN ( SELECT c.oid
                   , s.nspname AS referenced_namespace
                   , t.relname AS referenced_relation
                   , a.attname AS referenced_attribute
                   , false AS manual 
                FROM pg_constraint c
                JOIN pg_namespace  n ON n.oid = c.connamespace
                JOIN pg_class      t ON t.oid = c.confrelid
                JOIN pg_namespace  s ON s.oid = t.relnamespace
                JOIN pg_attribute  a ON a.attrelid = c.confrelid
                                    AND a.attnum = c.confkey[1]
               WHERE contype = 'f'
       ) referenced USING (oid)
 WHERE referrer_namespace NOT IN ( 'artiva'
                                 , 'bi'
                                 , 'bi_dev'
                                 , 'cba'
                                 , 'cnu_stats'
                                 , 'cnu_report'
                                 , 'code_internal'
                                 , 'constraint_check'
                                 , 'dbcheck'
                                 , 'import'
                                 , 'inheritance'
                                 , 'jzhang'
                                 , 'logs'
                                 , 'mef'
                                 , 'loc'
                                 , 'lead_sale'
                                 , 'londiste'
                                 , 'pgq'
                                 , 'pgq_ext'
                                 , 'ppc'
                                 , 'replication'
                                 , 'security'
                                 , 'tools'
                                 , 'to_delete'
                                 )
 ORDER BY referrer_namespace
        , referrer_relation
;
