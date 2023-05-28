import datetime
from datetime import datetime, timedelta
from pymongo.collection import Collection
from typing import Tuple, List
from helper_functions import *
from concurrent.futures import ThreadPoolExecutor, wait
from logger import WebScraperLogger
import asyncio

football_competitions = ["England Premier League", "England Championship", "England League 1", "England League 2", "England National League", "England FA Cup", "Germany Bundesliga", "Germany Bundesliga 2", "Germany Liga 3", "Germany Cup", "France League 1", "France League 2", "France Cup", "France National", "Spain La Liga", "Spain Cup", "Spain Segunda Division", "Spain Segunda Division RFEF Group 1", "Spain Segunda Division RFEF Group 2", "Spain Segunda Division RFEF Group 3", "Spain Segunda Division RFEF Group 4", "Spain Segunda Division RFEF Group 5", "Spain Primera Division RFEF Group 1", "Spain Primera Division RFEF Group 2", "Italy Cup", "Italy Serie A", "Italy Serie B", "Netherlands Eredivisie", "Netherlands Eerste Divisie", "Netherlands Cup", "USA MLS", "USA USL", "UEFA Champions League", "UEFA Europa League", "UEFA Conference League", "UEFA Nations League", "Belgium Cup", "Belgium Jupiler League", "Scotland Premier League", "Scotland Championship", "Scotland Challenge Cup", "Scotland League 1", "Scotland League 2", "Turkey SuperLiga", "Turkey TFF 1 Lig", "Switzerland SuperLeague", "Switzerland Challenge League", "Denmark SuperLiga", "Denmark Cup", "Denmark 1st Division", "Denmark 2nd Division", "Greece SuperLeague", "Greece SuperLeague 2", "Portugal Primeira Liga", "Portugal Segunda Liga", "Ireland Premier League", "Ireland Division 1", "Russia Premier League", "Russia Cup", "Austria 2 Liga", "Austria Bundesliga", "Austria Cup", "Australia A League", "Australia NPL Queensland", "Australia NPL Victorian", "Australia NPL Tasmania", "Australia NPL South Australia", "Australia NPL Western Australia", "Australia NPL New South Wales", "Australia NPL Capital Football", "Argentina Liga Pro", "Argentina Cup", "Argentina Reserve League", "Argentina Primera B Nacional", "Argentina Primera B Metropolitana", "Japan J League", "Japan J League Division 2", "Brazil Serie A", "Brazil Serie B", "Brazil Serie C", "Brazil Copa Do Nordeste", "Bahrain Premier League", "Bulgaria Parva Liga", "Chile Primera Division", "Chile Primera B", "China Super League", "Colombia Primera A", "Colombia Primera B", "Colombia Cup", "Cyprus First Division", "Czech Liga 1", "Ecuador Serie A", "Ecuador Serie B", "Egypt Premiership", "Finland Ykkonen", "Finland Veikkausliiga", "Iceland Urvalsdeild", "Mexico Primera Division", "Norway Eliteserien", "Norway First Division", "Poland Ekstraklasa", "Poland Liga 1", "Qatar Stars League", "SaudiArabia Premier League", "SouthKorea K League 1", "SouthKorea K League 2", "SouthAfrica Premier League", "Sweden Allsvenskan", "Sweden Cup", "Sweden Superettan", "Ukraine Premier League", "Thailand League 1", "Bolivia Liga Profesional", "Estonia Meistriliiga", "Iran Persian Gulf Pro League", "Latvia Virsliga", "Lithuania A Lyga", "Paraguay Primera Division", "Peru Primera Division", "Romania Liga 1", "Malaysia Super League", "Slovakia Super Liga", "Uruguay Primera Division", "Venezuela Primera Division", "Singapore Premier League", "UAEmirates Arabian Gulf League", "Croatia HNL", "Andorra Premier Division", "Armenia Premier League", "Georgia Erovnuli Liga", "Oman Professional League", "Kuwait Premier League", "Canada Premier League", "Hungary NB I", "Vietnam V.League 1"]


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
        Accepts a Tupple of Collections and returns a list of all value bets found in the collections within the next 7 days
        The first collection in the pair is the pinnacle collection
        """
        
        now = date = datetime.utcnow()
        date_string = now.strftime("%d/%m/%y")
        ids_of_updated_value_bets = []
        date_range = 3
        if self.line == 1:
            date_range = 1

        for _ in range(0 ,date_range):
            
            try:
                
                tasks = []
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                async def run_find_value(date_string, competition, collection_pair):
                    try:
                        result = await self.__find_value_bets_and_update_db(date_string, competition, collection_pair)
                        return result
                    except Exception as e:
                        self.logger.error(f"An exception of type {type(e).__name__} occurred while running __find_arbs_and_update_db: {str(e)}")
                        return None
                
                tasks = [run_find_value(date_string, competition, collection_pair) for competition in football_competitions]
                results = loop.run_until_complete(asyncio.gather(*tasks))
                loop.close()
                
                for result in results:
                    ids_of_updated_value_bets.extend(result)
                    
                date = date + timedelta(days=1)
                date_string = date.strftime("%d/%m/%y")
                
            except Exception as e:
                self.logger.error(f"An exception of type {type(e).__name__} occurred while running _find_value_bets_and_update_db: {str(e)}")
            
        return ids_of_updated_value_bets

    async def __find_value_bets_and_update_db(self, date_string: str, competition: str, collection_pair: Tuple[Collection, Collection]) -> list:
        
        match_pair_list = []
        now = datetime.utcnow()
        ids_of_updated_value_bets = []
        now_plus_two_hours = now + timedelta(hours=2)
        
        self.logger.info(f"Fetching matches for {competition} on {date_string}")
        collection1_matches = list(collection_pair[0].find({"date": date_string, "competition": competition}))
        collection2_matches = list(collection_pair[1].find({"date": date_string, "competition": competition}))
        
        if len(collection1_matches) <= len(collection2_matches):
            for _match1 in collection1_matches:
                if _match1['date'] ==  now.strftime("%d/%m/%y") and now_plus_two_hours.strftime("%H:%M") > _match1['time']:
                    pass
                else:
                    _match2 = find_similar_football_match(collection2_matches, _match1)
                    match_pair_list.append((_match1, _match2))
        else:
            for _match2 in collection2_matches:
                if _match2['date'] ==  now.strftime("%d/%m/%y") and now_plus_two_hours.strftime("%H:%M") > _match2['time']:
                    pass
                else:
                    _match1 = find_similar_football_match(collection1_matches, _match2)
                    match_pair_list.append((_match1, _match2))
                    
        if match_pair_list:
                
            self.logger.info("Determining value_bets for match pairs in match_pair_list")
            for match_pair in match_pair_list:
                
                try:
                    
                    match1, match2 = match_pair
                    
                    if match1 and match2:
                        
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
            
        return  ids_of_updated_value_bets