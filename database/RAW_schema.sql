DataBase Schema Raw and Cleaned

--Must be made prior to the table
CREATE TYPE gtfs_schedule_relationships AS ENUM (
    'SCHEDULED', 'CANCELED', 'ADDED', 
    'UNSCHEDULED', 'DUPLICATED', 'NEW', 
    'SKIPPED', 'NO_DATA'
);

CREATE TYPE transport AS ENUM (
    'TTC', 'GO'
);

--Note about timestamps: header’s timestamp is the time of the instance being loaded and timestamp within each stop_updates section is the time of the data itself being updated. They are both INTEGERS since they denote the time in Unix. 

-- One of either trip or route have to exist
CREATE TABLE RAW_trip_updates (
	agency transport NOT NULL,
	trip_entity_id TEXT NOT NULL,
	instance_timestamp INTEGER NOT NULL,
	data_update_timestamp INTEGER NOT NULL,
	fetched_time TIMESTAMPTZ DEFAULT NOW(),
    trip_id TEXT ,
    schedule_relationship gtfs_schedule_relationships DEFAULT 'SCHEDULED',
    route_id TEXT,
    stop_updates JSONB,

    PRIMARY KEY (agency,trip_entity_id, fetched_time)
);

ALTER TABLE RAW_trip_updates
ADD CONSTRAINT route_or_trip_id_exists
CHECK (trip_id IS NOT NULL OR route_id IS NOT NULL);

CREATE TYPE vehicle_status AS ENUM (
    'IN_TRANSIT_TO', 'INCOMING_AT', 'STOPPED_AT'
);

CREATE TABLE RAW_vehicle_positions (
	agency transport NOT NULL,
	vehicle_entity_id TEXT NOT NULL,
    vehicle_id TEXT, 
	instance_timestamp INTEGER NOT NULL,
	data_update_timestamp INTEGER,
	fetched_time TIMESTAMPTZ DEFAULT NOW(),
	trip_id TEXT,
    schedule_relationship gtfs_schedule_relationships DEFAULT 'SCHEDULED',	route_id TEXT, 
    current_stop_sequence SMALLINT,
    current_status vehicle_status, 
    stop_id TEXT,
    position JSONB NOT NULL,

    PRIMARY KEY (agency, vehicle_entity_id, fetched_time)
);