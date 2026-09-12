"""
[ Your Script ] ──( HTTP GET )──> 
[ Open Data API ] ──( Binary Data )──> 
[ Protocol Buffer Decoder ] ──> 
[ Structured Object ]
"""

import asyncio
import aiohttp
import logging
import json

from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
from datetime import date
from enum import Enum

from database import DatabaseManager
from extraction import (trip_extraction, vehicle_extraction)

class agency(Enum):
    GO = 1
    TTC = 2

class information_type(Enum):
    TRIP_UPDATE = 1
    VEHICLE_UPDATE = 2 

def setup_logging():
    logging.basicConfig(
        filename="../logs/polling_logs.log",
        filemode="a",  # 'a' to append, 'w' to overwrite
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

# Initializes the global configuration for Python's built-in logging module
# Module global logger & have it write in append mode to the log file
setup_logging()
logger = logging.getLogger(__name__)


class Poller:
    def __init__(self, agency:agency, request_link:str, info_type:str) -> None:
        self.agency = agency
        self.request_link = request_link
        self.information_type = info_type
        self.feed: gtfs_realtime_pb2.FeedMessage = gtfs_realtime_pb2.FeedMessage()
        self.timeout = aiohttp.ClientTimeout(total=14.0)


    async def fetch_data(self):
        # Make the 
        async with aiohttp.ClientSession() as session:
        # Make the asynchronous GET request to the specified link
            try:
                async with session.get(self.request_link, timeout=self.timeout) as response:
                    # Raise for the status of the request immediately 
                    response.raise_for_status()

                    logger.info(
                        "Feed fetched successfully: agency=%s information_type=%s status=%s",
                        self.agency.name,
                        self.information_type.name,
                        response.status,
                    )
                    
                    # Await the text or JSON content
                    self.raw_data = await response.read()
                    self._decode_information()
                    
            except aiohttp.ClientResponseError as error:
                # You access the numeric code using error.status (not error.response.status_code)
                logger.error(
                    "HTTP request failed: agency=%s type=%s status=%s",
                    self.agency.name,
                    self.information_type.name,
                    error.status,
                    exc_info=True,
                )
                raise
            except aiohttp.ClientError as error:
                # Captures other generic network/connection issues
                logger.error(
                    "Network error: agency=%s type=%s",
                    self.agency.name,
                    self.information_type.name,
                    exc_info=True,
                )
                raise
            except asyncio.TimeoutError as error:
                # Catches an aiohttp timeout error
                logger.error(
                    "Connected timed out: agency=%s type=%s",
                    self.agency.name,
                    self.information_type.name,
                    exc_info=True,
                )
                raise

    def _decode_information(self):
        if not self.raw_data:
            return None
        
        # The feed should now be de-serialized
        # The feed is modified in place, and returns the byte count!
        # Catch a malformed protobuf if necessary
        try:
            self.feed.ParseFromString(self.raw_data)
        except DecodeError as error:
            logger.error(
                "Malformed protobuf error: agency=%s type=%s",
                self.agency.name,
                self.information_type.name,
                exc_info=True,
            )
            raise
        if not bool(self.feed.ListFields()):
            logger.info(
                "Empty feed: agency=%s type=%s",
                self.agency.name,
                self.information_type.name,
            )
            
    def clear_feed(self):
        """Called externally (manager class) to clear the feed"""
        if self.feed:
            self.feed.Clear() 


async def insert_rows(db_manager: DatabaseManager, table:str, columns:list[str], data:list[tuple], json_cols:list[str]) -> None:
    # Setup implies that the columns and the data values must be in the same order 
    placeholders_str = ", ".join([f"${n+1}" if val not in json_cols else f"${n+1}::jsonb" for n,val in enumerate(columns)])
    column_str = ", ".join(columns)
    
    query = f"INSERT INTO {table} ({column_str}) VALUES ({placeholders_str})"

    await db_manager._write_to_database(query, data)

class RawDataManager:
    def __init__(self, database_manager):
        self.database_manager = database_manager

        # self.go_trip_poller = Poller(agency.GO, "TBD", information_type.TRIP_UPDATE)
        self.ttc_trip_poller = Poller(agency.TTC, "https://bustime.ttc.ca/gtfsrt/trips?", information_type.TRIP_UPDATE)
        # self.go_vehicle_poller = Poller(agency.GO, "TBD", information_type.VEHICLE_UPDATE)
        self.ttc_vehicle_poller = Poller(agency.TTC, "https://bustime.ttc.ca/gtfsrt/vehicles?", information_type.VEHICLE_UPDATE)

        self.pollers = [
            # self.go_trip_poller, self.go_vehicle_poller, 
            self.ttc_trip_poller, self.ttc_vehicle_poller
        ]

    async def _single_poller_cycle(self,poller):
        await poller.fetch_data()

        # Organize data for insertion
        if poller.information_type == information_type.TRIP_UPDATE:
            table = "RAW_trip_updates"
            columns = [
                "agency", "trip_entity_id", "instance_timestamp", "data_update_timestamp", 
                "trip_id", "route_id", "schedule_relationship", "stop_updates"
            ]

            logger.info(
                "Beginning trip extraction"
            )
            rows = trip_extraction(poller.agency, poller.feed)

        else:
            table = "RAW_vehicle_positions"
            columns = [
                "agency", "vehicle_entity_id", "vehicle_id", "instance_timestamp", 
                "data_update_timestamp", "trip_id", "route_id", "schedule_relationship", 
                "current_stop_sequence", "current_status", "stop_id", "position"
                
            ]
            rows = vehicle_extraction(poller.agency, poller.feed)

        # Insert data accordingly 
        await insert_rows(self.database_manager, table, columns, rows, ["position", "stop_updates"])

        # This should only run if there are no exceptions, preserves any unwritten feed information
        poller.clear_feed()

    async def run_cycle(self):
        # Record the start of the cycle, reset successes and fails: 
        cycle_start = asyncio.get_running_loop().time()
        successes = fails = 0
        
        # This method runs a single cycle
        # Crete a task for each of the pollers
        tasks = [self._single_poller_cycle(poller) for poller in self.pollers]

        # Gather them so they run at the same time | this returns something / task
        cycle_attempt = await asyncio.gather(*tasks, return_exceptions=True)

        # This catches the exceptions raised inside _single_poller_cycle
        for res in cycle_attempt:
            if isinstance(res, Exception):
                fails += 1 
                logging.error(
                    f"Caught async error: {res}",
                    exc_info=True
                )
            else:
                successes += 1 
        
        # Difference between start and end time
        cycle_time = asyncio.get_running_loop().time() - cycle_start

        # Summary log!
        logger.info(
            "Cycle completed: successful=%s failed=%s duration=%.2fs",
            successes,
            fails,
            cycle_time,
        )

    
# "Main" so to speak
# 1. Instantiate, which makes all necessary objects
async def start_poller():
    loop = asyncio.get_running_loop()
    interval = 15.0  # Target interval in seconds

    # Instantiate the only ever db manager
    database_manager = DatabaseManager()
    raw_data_manager = RawDataManager(database_manager)

    logging.info("Scheduler started. Target interval: {interval} seconds.")

    while True:
        # Record the exact internal loop time before the task runs
        start_time = loop.time()
        
        # Run the cycle one time! 
        await raw_data_manager.run_cycle()
        
        # Calculate how long the task actually took to execute
        execution_time = loop.time() - start_time
        
        # Dynamically calculate the remaining sleep time needed to hit the 15s mark
        sleep_time = max(0.0, interval - execution_time)
        
        logging.info(f"--> Job took {execution_time:.2f}s. Sleeping for {sleep_time:.2f}s...\n")

        # Sleep for the remaining time
        await asyncio.sleep(sleep_time)

asyncio.run(start_poller())