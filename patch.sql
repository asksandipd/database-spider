/*
The zoo schema is for a program called the Customer Zoo Tool.
The tool reads metadata about the object graph for a customer in the database,
extracts the data from one database, scrubs it to remove all Personally Identifiable Information (PII),
and then inserts the clean data into another database.

The tables in the zoo schema present the metadata in an easily-digestible format.
The data in the zoo.object_dependencies table may be generated in part by a program, but a certain amount
of manual intervention is expected too.  There's a flag identifying the manually-inserted rows.

The zoo.pii table will almost certainly be hand-populated.  
*/

CREATE SCHEMA zoo;
ALTER SCHEMA zoo OWNER TO cnuadmin;

CREATE TABLE zoo.object_dependencies
(
        dependent_schema TEXT
,       dependent_table  TEXT
,       dependent_column TEXT
,       referenced_schema TEXT
,       referenced_table TEXT
,       referenced_column TEXT
,       manual BOOLEAN
,       CONSTRAINT object_dependencies__pkey PRIMARY KEY
        ( dependent_schema
	, dependent_table
	, dependent_column
	, referenced_schema
	, referenced_table
	, referenced_column
	)
);
ALTER TABLE zoo.object_dependencies OWNER TO cnuadmin;

COMMENT ON TABLE zoo.object_dependencies IS $$Metadata for the Customer Zoo Tool.  This table describes the foreign key relationships between tables in the object graph for customers, loans, approvals, credit_reports, and anything else relating to a customer.  A program updates this, but some rows are inserted manually, hence the manual flag.  The manually inserted rows describe relationships that the database doesn't know about yet.  When the database is fully self-describing, we won't need this table any longer.$$;

CREATE TABLE zoo.pii
(
        pii_schema               TEXT        NOT NULL
,       pii_table                TEXT        NOT NULL
,       pii_column               TEXT        NOT NULL
,       pii_proxy_value          TEXT   
,	pii_uniquely_constrained BOOLEAN     NOT NULL DEFAULT FALSE
,       CONSTRAINT pii__pkey PRIMARY KEY
        ( pii_schema
	, pii_table
	, pii_column
	)
);
ALTER TABLE zoo.pii OWNER TO cnuadmin;

COMMENT ON TABLE zoo.pii                            is $$Characteristics of Personally Identifiable Information (PII) that may be used for scrubbing.  The schema, table, and column uniquely identify a datum that is considered PII.  An optional proxy value, if supplied, specifies the value to use instead of the value stored in the column.  A boolean declares whether the value is uniquely constrained.$$;
COMMENT ON COLUMN zoo.pii.pii_schema                is $$The schema in which the value is found.$$;
COMMENT ON COLUMN zoo.pii.pii_table                 is $$The table in which the value is found.$$;
COMMENT ON COLUMN zoo.pii.pii_column                is $$The column in which the value is found.$$;
COMMENT ON COLUMN zoo.pii.pii_proxy_value           is $$An optional proxy value that may be used instead of the actual value.  The representation is a string literal suitable for use in an INSERT statement.  Possible values include the empty string (''), a number ('1' or '3.50') or a date ('2010-10-31 23:59:59').  A NULL pii_proxy_value indicates that a replacement value must be constructed by other means.$$;
COMMENT ON COLUMN zoo.pii.pii_uniquely_constrained is $$Boolean is true if the column has a unique constraint.$$;

/* FIXME!!!
GRANT ALL ON schema zoo TO cnuapp;
GRANT ALL ON zoo.object_dependencies TO cnuapp;
GRANT ALL ON zoo.pii TO cnuapp;

UPDATE zoo.object_dependencies 
SET dependent_column   = 'person_id', 
    referenced_schema  = 'private', 
    referenced_table   = 'people'
    manual             = true
WHERE dependent_table  = 'customers'
AND   referenced_table = 'brands';
*/
