import datetime
from datetime import datetime, timedelta, time
from pymongo.collection import Collection
from typing import Tuple, List
from helper_functions import *
from concurrent.futures import ThreadPoolExecutor, wait
from logger import WebScraperLogger
import math
from more_itertools import chunked
import asyncio

class ValueBetFinder:
    """ 
    Description
    -------------- 
    ValueBetFinder scans through different bookmakers comparing their odds with pinnacle odds, to find value bets an dupdate the value bet database
    
    Parameters
    ------------
    - paired_collections: List[Tuple[Collection, Collection]
        This is the list of the paired collections which the ValueBetFinder has to scan
    - value_bet_collection: Collection
        This this the value_bet_collection where value bets are stored
    - logger: WebScraperLogger
        The logger to log any information while finding value_bets
    - thread_pool_workers: int
        The number of threads the ValueBetFinder should use (more threads increases the scan speed)
        
    Methods
    -------------
    - find_value_bets_and_update_db() -> int
        This method scans through all the paired_collections and updates the value_bet_collection with any value bets found
    """
    
    ids_of_updated_value_bets = []
    
    def __init__(
                 self,
                 paired_collections: List[Tuple[Collection, Collection]], 
                 value_bet_collection: Collection,
                 logger: WebScraperLogger,
                 line: int,
                 competitions: list,
                 thread_pool_workers = 0,
                 date_range = 3
                 ):
        
                self.paired_collections = paired_collections
                self.logger = logger
                self.value_bet_collection = value_bet_collection
                self.thread_pool_workers = thread_pool_workers
                self.line = line
                self.competitions = competitions
                self.date_range = date_range
        
    def find_value_bets_and_update_db(self) -> int:
        
        """ 
        Description
        -------------
        find_value_bets_and_update_db, finds all value bets present in the paired_collections and updates the value_bet_collection
        
        Returns
        ----------- 
        - int: the number of value bets present in the paired_collections
        """
        
        self.logger.info("Starting Value Bet Finder ...")
        
        try:
            
            #calculating threadpool workers for parrallel execution
            if self.thread_pool_workers == 0:
                max_workers = len(self.paired_collections)
            else:
                max_workers = self.thread_pool_workers
            
            #starting the threadpool for the collection pairs
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                
                for collection_pair in self.paired_collections:                   
                    executor.submit(self.split_search_by_collection_pair, collection_pair)
                
            self.value_bet_collection.delete_many({'_id': {'$nin': self.ids_of_updated_value_bets}})  #delete any value bet that is no longer available from the value_bet_collection
                  
        except Exception as e:
            self.logger.error(f"An exception of type {type(e).__name__} occurred while running find_value_bets_and_update_db: {str(e)}")
        
        return len(self.ids_of_updated_value_bets)

    def split_search_by_collection_pair(self, collection_pair: Tuple[Collection, Collection]):
        
        """ 
        Accepts a Tupple of Collections and returns a list of all value bets found in the collections within the next self.date_range days
        The first collection in the pair is the pinnacle collection
        """
        
        now = date = datetime.utcnow()
        date_string = now.strftime("%d/%m/%y")
        
        date_range = self.date_range
        if self.line == 1:
            date_range = 1

        for _ in range(0 ,date_range):
            
            try:
                
                #calculating threadpool workers for parrallel execution
                if self.thread_pool_workers == 0:
                    max_workers = len(self.competitions)
                else:
                    max_workers = self.thread_pool_workers/len(self.paired_collections) - 1 #len(self.paried_collections) workers have already been used above
                    if max_workers < 1:
                        raise Exception("The number of workers provided are not enough to make the search")
                
                #chunk the competitions depending on the number of workers
                chunck_length = math.ceil(len(self.competitions)/max_workers)
                competition_chuncks = list(chunked(self.competitions, chunck_length))

                #starting the threadpool for the competition_chuncks
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    
                    for competition_list in competition_chuncks:
                        executor.submit(self.split_search_by_competitions, competition_list, date_string, collection_pair) 
                    
                date = date + timedelta(days=1)
                date_string = date.strftime("%d/%m/%y")
                
            except Exception as e:
                self.logger.error(f"An exception of type {type(e).__name__} occurred while running _find_value_bets_and_update_db: {str(e)}")

    def split_search_by_competitions(self, football_competition_list: list, date_string: str, collection_pair: Tuple[Collection, Collection]):
        
        #configuration for asyncio 
        tasks = []
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        async def run_find_value(date_string, competition, collection_pair):
            try:
                await self._find_value_bets_and_update_db(date_string, competition, collection_pair)
            except Exception as e:
                self.logger.error(f"An exception of type {type(e).__name__} occurred while running _find_value_bets_and_update_db: {str(e)}")
        
        tasks = [run_find_value(date_string, competition, collection_pair) for competition in football_competition_list]
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
        
    async def _find_value_bets_and_update_db(self, date_string: str, competition: str, collection_pair: Tuple[Collection, Collection]):
        
        match_pair_list = []
        now = datetime.utcnow()
        now_plus_two_hours = now + timedelta(hours=2)
        start_time = time(0, 0)  # 00:00
        end_time = time(1, 0)  # 01:00
        if start_time <= now.time() <= end_time:
            time_ref = start_time
        else:
            time_ref = (now - timedelta(hours=1)).time()
        
        collection1_matches = list(collection_pair[0].find({"date": date_string, "country": competition}))
        collection2_matches = list(collection_pair[1].find({"date": date_string, "country": competition}))
        time_format = '%H:%M:%S'
        date_format = "%d/%m/%y"
        
        if len(collection1_matches) <= len(collection2_matches):
            for _match1 in collection1_matches:
                if  datetime.strptime(_match1['last_modified_date'], date_format).date() < now.date():
                    pass
                elif _match1['date'] ==  now.strftime("%d/%m/%y") and now_plus_two_hours.strftime("%H:%M") > _match1['time']:
                    pass
                else:
                    _match2 = find_similar_football_match(collection2_matches, _match1)
                    if _match2:
                        if  datetime.strptime(_match2['last_modified_date'], date_format).date() >= now.date():
                            match1_last_modified_time = datetime.strptime(_match1['last_modified_time'], time_format).time()
                            match2_last_modified_time = datetime.strptime(_match2['last_modified_time'], time_format).time()
                            if match1_last_modified_time > time_ref and  match2_last_modified_time > time_ref:
                                match_pair_list.append((_match1, _match2))
        else:
            for _match2 in collection2_matches:
                if  datetime.strptime(_match2['last_modified_date'], date_format).date() < now.date():
                    pass
                elif _match2['date'] ==  now.strftime("%d/%m/%y") and now_plus_two_hours.strftime("%H:%M") > _match2['time']:
                    pass
                else:
                    _match1 = find_similar_football_match(collection1_matches, _match2)
                    if _match1:
                        if  datetime.strptime(_match1['last_modified_date'], date_format).date() >= now.date():
                            match1_last_modified_time = datetime.strptime(_match1['last_modified_time'], time_format).time()
                            match2_last_modified_time = datetime.strptime(_match2['last_modified_time'], time_format).time()
                            if match1_last_modified_time > time_ref and  match2_last_modified_time > time_ref:
                                match_pair_list.append((_match1, _match2))
                
        for match_pair in match_pair_list:
            
            try:
                
                match1, match2 = match_pair
                
                if match1 and match2:
                    
                    if match1["last_modified_date"] == match2["last_modified_date"]:
                        
                        match1, match2 = equalize_matches(match1, match2)
                        match1 = update_match_scoreboard(match1, no_vig_odds)
                        final_match = remove_false_fields(compare_markets(match1, match2, match1['bookmaker_name'], match2['bookmaker_name']))
                        
                        if 'scoreboards' in final_match:
                            result = self.value_bet_collection.replace_one({'_id': final_match['_id']}, final_match, upsert=True)
                            if result:
                                self.ids_of_updated_value_bets.append(final_match['_id'])
                            else:
                                self.logger.error(f"A Value bet could not be added to database: {final_match}, result: {result}")
                
            except Exception as e:
                self.logger.error(f"An exception of type {type(e).__name__} occurred while comparing markets in match_pair: {str(e)}")