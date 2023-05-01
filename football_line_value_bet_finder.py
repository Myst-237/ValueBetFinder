import pymongo
from value_bet_finder import ValueBetFinder
from logger import WebScraperLogger

LOG_PATH = 'logs.log'
logger = WebScraperLogger(name=__name__, log_file_path=LOG_PATH)

MONGODB_PWD = 'b6oKqvjLlbNE199G'
MONGODB_URL = f'mongodb+srv://cadellgriffith:{MONGODB_PWD}@bookiemarkets.vjfi1ot.mongodb.net/?retryWrites=true&w=majority'

client = pymongo.MongoClient(MONGODB_URL)
match_db = client["BookieMarkets"]
value_db = client["ValueBets"]
xbet_football_collection = match_db["xbet_line_football_collection"]
stake_football_collection = match_db["stake_line_football_collection"]
pinnacle_football_collection = match_db["pinnacle_line_football_collection"]
football_value_bet_collection =  value_db["football_line_value_bet_collection"]
paired_collections = [(pinnacle_football_collection, xbet_football_collection), (pinnacle_football_collection, stake_football_collection)]

value_bet_finder = ValueBetFinder(
                        paired_collections=paired_collections, 
                        value_bet_collection=football_value_bet_collection,
                        logger=logger,
                        thread_pool_workers=1,
                        line=0
                        )

if __name__ == "__main__":
    
    while True:
        
        try:
            
            value_bets_found = value_bet_finder.find_value_bets_and_update_db()
            logger.info(f"{value_bets_found} value bets for upcoming football matches were found and added to db")
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"An exception of type {type(e).__name__} occurred while finding arbs: {str(e)}")