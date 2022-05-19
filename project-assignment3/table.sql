drop table if exists stop_event;

create table stop_event (
        trip_id integer,
        vehicle_id integer,
        leave_time integer,
        train integer,
        route_id integer,
        direction integer,
        stop_time integer,
        arrive_time integer,
        dwell integer,
        location_id integer,
        door integer,
        lift integer,
        ons integer,
        offs integer,
        estimated_load integer,
        maximum_speed float,
        train_mileage float,
        pattern_distance float,
        location_distance float,
        x_coordinate float,
        y_coordinate float,
        data_source integer,
        schedule_status integer,
        PRIMARY KEY (trip_id, vehicle_id, leave_time)
);

create index trip_id_idx on stop_event(trip_id);