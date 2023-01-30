create schema if not exists landing;
create table if not exists landing.mongo_raw (
	valid_from_dt timestamptz not null default clock_timestamp(),
	valid_to_dt timestamptz not null default 'Infinity',

	id bytea not null,
	event_dt timestamptz not null,
	type text not null,
	operation text not null,
	object jsonb not null,
	primary key (id, type, valid_to_dt)
);

create unique index if not exists mr_valid_from on landing.mongo_raw(valid_from_dt, type, id);

create or replace function landing.mongo_type_2() returns trigger as $$
BEGIN
    -- Handle all txns BEFORE they are written
    IF TG_OP = 'INSERT' AND TG_WHEN = 'BEFORE' THEN
        NEW.valid_to_dt = 'Infinity';
        NEW.valid_from_dt = clock_timestamp();

	RAISE NOTICE 'Executing the insert trigger %s', NEW;

	update landing.mongo_raw set
		valid_to_dt = NEW.valid_from_dt
	where
		id = NEW.id
	and	type = NEW.type
	and	valid_to_dt = 'Infinity';

        RETURN NEW;
    ELSIF TG_OP = 'UPDATE' AND TG_WHEN = 'BEFORE' THEN
        -- IF OLD.valid_to_dt != 'Infinity' THEN
        --     RAISE EXCEPTION '% on %.%.% : attempting to update an older record: % vs %', TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NAME, OLD, NEW;
        -- END IF;

        -- IF NEW.valid_from_dt < OLD.valid_from_dt THEN
        --     RAISE EXCEPTION '% on %.%.% : attempting to add a record out of order: % vs %', TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_NAME, OLD, NEW;
        -- END IF;

	-- RAISE NOTICE 'Executing the update trigger %s -> %s', OLD, NEW;

        RAISE EXCEPTION '% in % on %.% has been disabled', TG_OP, TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME;
	return NEW;
    ELSIF TG_OP = 'DELETE' AND TG_WHEN = 'BEFORE' THEN
        RAISE EXCEPTION '% in % on %.% has been disabled', TG_OP, TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME;
        RETURN OLD;

    ELSIF TG_OP = 'TRUNCATE' AND TG_WHEN = 'BEFORE' THEN
        RAISE EXCEPTION '% in % on %.% has been disabled', TG_OP, TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME;

    -- Handle all txns AFTER the write
    ELSIF TG_OP = 'INSERT' AND TG_WHEN = 'AFTER' THEN
        RETURN NULL;

    ELSIF TG_OP = 'UPDATE' AND TG_WHEN = 'AFTER' THEN
	-- OLD.valid_to_dt = NEW.valid_from_dt;
	-- insert into landing.mongo_raw select OLD.*;

        RETURN NULL;

    ELSIF TG_OP = 'DELETE' AND TG_WHEN = 'AFTER' THEN
        RETURN NULL;

    ELSE
        RAISE EXCEPTION 'Unhandled transaction type % in %.% on %.%', TG_OP, TG_WHEN, TG_NAME, TG_TABLE_SCHEMA, TG_TABLE_NAME;

    END IF;
END;
$$ LANGUAGE plpgsql;

drop trigger if exists MM_mongo_raw_noop on landing.mongo_raw;
create trigger MM_mongo_raw_noop
before update
on landing.mongo_raw
for each row
execute procedure suppress_redundant_updates_trigger();

drop trigger if exists NM_mongo_raw_type_2 on landing.mongo_raw;
create trigger NM_mongo_raw_type_2
before insert or update or delete
on landing.mongo_raw
for each row
when (pg_trigger_depth() = 0)
execute procedure landing.mongo_type_2();

drop trigger if exists OM_mongo_raw_type_2 on landing.mongo_raw;
create trigger OM_mongo_raw_type_2
after insert or update or delete
on landing.mongo_raw
for each row
when (pg_trigger_depth() = 0)
execute procedure landing.mongo_type_2();


