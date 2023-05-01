import datetime
from datetime import datetime, timedelta
from pymongo.collection import Collection
from typing import Tuple, List
from helper_functions import remove_false_fields, compare_markets, equalize_matches, update_match_scoreboard, no_vig_odds
from concurrent.futures import ThreadPoolExecutor, wait
from logger import WebScraperLogger

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
    
    def __init__(
                 self,
                 paired_collections: List[Tuple[Collection, Collection]], 
                 value_bet_collection: Collection,
                 logger: WebScraperLogger,
                 line: int,
                 thread_pool_workers = 0,
                 ):
        
                self.paired_collections = paired_collections
                self.logger = logger
                self.value_bet_collection = value_bet_collection
                self.thread_pool_workers = thread_pool_workers
                self.line = line
        
    def find_value_bets_and_update_db(self) -> int:
        
        """ 
        Description
        -------------
        find_value_bets_and_update_db, finds all value bets present in the paired_collections and updates the value_bet_collection
        
        Returns
        ----------- 
        - int: the number of value bets present in the paired_collections
        """
        
        ids_of_updated_value_bets = []
        
        try:
            
            self.logger.info("Finding Value bets ...")
            if self.thread_pool_workers == 0:
                max_workers = len(self.paired_collections)
            else:
                max_workers = self.thread_pool_workers
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                                        
                futures = [executor.submit(
                                            self._find_value_bets_and_update_db,
                                            collection_pair, 
                                            )
                                            for collection_pair in self.paired_collections]
                
                completed_tasks, incomplete_tasks = wait(futures, timeout=600)
                results = [task.result() for task in completed_tasks]
                
                for result in results:
                    try:
                        if result:
                            ids_of_updated_value_bets.extend(result)  
                    except Exception as e:
                        self.logger.error(f"An exception of type {type(e).__name__} occurred while processing a future object in find_value_bets_and_update_db: {str(e)}")
                
            self.value_bet_collection.delete_many({'_id': {'$nin': ids_of_updated_value_bets}})  #delete any value bet that is no longer available from the value_bet_collection
                  
        except Exception as e:
                        self.logger.error(f"An exception of type {type(e).__name__} occurred while running find_value_bets_and_update_db: {str(e)}")
        
        return len(ids_of_updated_value_bets)

    def _find_value_bets_and_update_db(self, collection_pair: Tuple[Collection, Collection]) -> list:
        
        """ 
        Accepts a Tupple of Collections and returns a list of all value bets found in the collections within the next 30 days
        The first collection in the pair is the pinnacle collection
        """
        
        now = date = datetime.utcnow()
        #now_minus_one_hour = now - timedelta(hours=1)
        date_string = now.strftime("%d/%m/%y")
        ids_of_updated_value_bets = []
        date_range = 30
        if self.line == 1:
            date_range = 1

        for _ in range(0 ,date_range):
            
            try:
                
                #query matches by dates
                self.logger.info(f"Fetching matches for {date_string}")
                collection1_matches = list(collection_pair[0].find({"date": date_string}))
                collection2_matches = {collection2_match["_id"]: collection2_match for collection2_match in collection_pair[1].find({"date": date_string})}
                
                if collection1_matches:
                    
                    match_pair_list = []
                    
                    self.logger.info(f"Adding matches to match_pair_list")
                    for collection1_match in collection1_matches:
                        
                        try:
                            if collection1_match['date'] ==  now.strftime("%d/%m/%y") and now.strftime("%H:%M") > collection1_match['time']:
                                pass
                            else:
                                if collection1_match["_id"] in collection2_matches:
                                    match_pair_list.append((collection1_match, collection2_matches[collection1_match["_id"]]))
                                    del collection2_matches[collection1_match["_id"]]
                        
                        except Exception as e:
                            self.logger.error(f"An exception of type {type(e).__name__} occurred while adding matches to match_pair_list {str(e)}")
                    
                    if match_pair_list:
                        
                        self.logger.info("Determining value_bets for match pairs in match_pair_list")
                        for match_pair in match_pair_list:
                            
                            try:
                                
                                match1, match2 = match_pair
                                
                                if match1["last_modified_date"] == match2["last_modified_date"]:
                                    
                                    match1, match2 = equalize_matches(match1, match2)
                                    match1 = update_match_scoreboard(match1, no_vig_odds)
                                    final_match = remove_false_fields(compare_markets(match1, match2, match1['bookmaker_name'], match2['bookmaker_name']))
                                    
                                    if 'scoreboards' in final_match:
                                        existing_match = self.value_bet_collection.find_one({'_id': final_match['_id']})
                                        if existing_match:
                                            self.value_bet_collection.delete_one({'_id': final_match['_id']})
                                        result = self.value_bet_collection.insert_one(final_match)
                                        if result:
                                            ids_of_updated_value_bets.append(final_match['_id'])
                                            self.logger.info(f"Value bet successfully added to line value bets, _id: {final_match['_id']}")
                                        else:
                                            self.logger.error(f"An exception of type {type(e).__name__} occurred while adding a match to the value collection: {str(e)}")
                            
                            except Exception as e:
                                self.logger.error(f"An exception of type {type(e).__name__} occurred while comparing markets in match_pair: {str(e)}")
                    
                    else:
                        self.logger.info(f"No matches on {date_string}")
                else:
                    self.logger.info(f"No matches on {date_string}")
                    
                date = date + timedelta(days=1)
                date_string = date.strftime("%d/%m/%y")
                
            except Exception as e:
                self.logger.error(f"An exception of type {type(e).__name__} occurred while running _find_value_bets_and_update_db: {str(e)}")
            
        return ids_of_updated_value_bets
            