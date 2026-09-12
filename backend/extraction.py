from google.transit import gtfs_realtime_pb2
import logging
import json

logger = logging.getLogger(__name__)

def trip_extraction(agency, feed) -> list[tuple]:
    all_trip_rows = [] 

    # Each feed contains one header and many entities
    for entity in feed.entity:
        # Ensure that there is a trip_update field 
        if not entity.HasField("trip_update"):
            continue

        # Extract the following information
        trip_entity_id = entity.id
        instance_timestamp = feed.header.timestamp
        data_update_timestamp = entity.trip_update.timestamp
        trip_id = entity.trip_update.trip.trip_id
        route_id = entity.trip_update.trip.route_id

        # This is an int in an enum and we have to change it to its value
        schedule_relationship =  gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(entity.trip_update.trip.schedule_relationship)

        # The stop_updates JSONB column
        stop_updates = []

        for stop_update in entity.trip_update.stop_time_update:

            stop_update_data = {
                "stop_sequence": (
                    stop_update.stop_sequence if stop_update.HasField("stop_sequence")
                    else None
                ),

                "stop_id": (
                    stop_update.stop_id
                    if stop_update.HasField("stop_id")
                    else None
                ),

                "schedule_relationship": (
                    stop_update.schedule_relationship
                    if stop_update.HasField("schedule_relationship")
                    else None
                ),

                "arrival": None,
                "departure": None,
            }

            # Arrival information
            if stop_update.HasField("arrival"):
                stop_update_data["arrival"] = {
                    "delay": (
                        stop_update.arrival.delay
                        if stop_update.arrival.HasField("delay")
                        else None
                    ),

                    "time": (
                        stop_update.arrival.time
                        if stop_update.arrival.HasField("time")
                        else None
                    ),

                    "uncertainty": (
                        stop_update.arrival.uncertainty
                        if stop_update.arrival.HasField("uncertainty")
                        else None
                    ),
                }

            # Departure information
            if stop_update.HasField("departure"):
                stop_update_data["departure"] = {
                    "delay": (
                        stop_update.departure.delay
                        if stop_update.departure.HasField("delay")
                        else None
                    ),

                    "time": (
                        stop_update.departure.time
                        if stop_update.departure.HasField("time")
                        else None
                    ),

                    "uncertainty": (
                        stop_update.departure.uncertainty
                        if stop_update.departure.HasField("uncertainty")
                        else None
                    ),
                }
            
            # Append the JSON like obj to the list
            stop_updates.append(stop_update_data)
        
        # Then dump to all the dicts in the list afterwards
        stop_updates = json.dumps(stop_updates)


        all_trip_rows.append((
            agency.name, trip_entity_id, instance_timestamp, data_update_timestamp,
            trip_id, route_id, schedule_relationship, stop_updates
        ))
    
    return all_trip_rows


def vehicle_extraction(agency, feed) -> list[tuple]:
    all_vehicle_rows = []

    for entity in feed.entity:

        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle
        vehicle_entity_id = entity.id
        vehicle_id = (
            vehicle.vehicle.id
            if vehicle.HasField("vehicle") and vehicle.vehicle.HasField("id")
            else None
        )
        instance_timestamp = feed.header.timestamp
        data_update_timestamp = (
            vehicle.timestamp
            if vehicle.HasField("timestamp")
            else None
        )
        trip_id = (
            vehicle.trip.trip_id
            if vehicle.HasField("trip") and vehicle.trip.HasField("trip_id")
            else None
        )
        route_id = (
            vehicle.trip.route_id
            if vehicle.HasField("trip") and vehicle.trip.HasField("route_id")
            else None
        )
        schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(vehicle.trip.schedule_relationship)
            if vehicle.HasField("trip")
            else None # SCHEDULED is default
        )
        current_stop_sequence = (
            vehicle.current_stop_sequence
            if vehicle.HasField("current_stop_sequence")
            else None
        )
        current_status = (
            gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(vehicle.current_status) 
            if vehicle.HasField("current_status")
            else None
        )
        stop_id = (
            vehicle.stop_id
            if vehicle.HasField("stop_id")
            else None
        )

        position = None

        if vehicle.HasField("position"):
            position = {
                "latitude": vehicle.position.latitude,
                "longitude": vehicle.position.longitude,

                "bearing": (
                    vehicle.position.bearing
                    if vehicle.position.HasField("bearing")
                    else None
                ),

                "odometer": (
                    vehicle.position.odometer
                    if vehicle.position.HasField("odometer")
                    else None
                ),

                "speed": (
                    vehicle.position.speed
                    if vehicle.position.HasField("speed")
                    else None
                ),
            }

        position = json.dumps(position)

        all_vehicle_rows.append((
            agency.name,vehicle_entity_id, vehicle_id, instance_timestamp, 
            data_update_timestamp, trip_id, route_id, schedule_relationship,
            current_stop_sequence, current_status, stop_id, position
        ))
    
    return all_vehicle_rows