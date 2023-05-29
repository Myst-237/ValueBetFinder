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
megapari_football_collection = match_db["megapari_line_football_collection"]
pinnacle_football_collection = match_db["pinnacle_line_football_collection"]
football_value_bet_collection =  value_db["football_line_value_bet_collection"]
paired_collections = [(pinnacle_football_collection, xbet_football_collection), (pinnacle_football_collection, stake_football_collection), (pinnacle_football_collection, megapari_football_collection)]

football_competitions = ["England Premier League", "England Championship", "England League 1", "England League 2", "England National League", "England FA Cup", "Germany Bundesliga", "Germany Bundesliga 2", "Germany Bundesliga 2 Women", "Germany Liga 3", "Germany Cup", "Germany Oberliga Mitterlrhein", "Germany Oberliga Niederrhein", "Germany Oberliga Nordost Sud", "Germany Oberliga Westfalen", "France League 1", "France League 2", "France Cup", "France National", "Spain La Liga", "Spain Cup", "Spain Segunda Division", "Spain Segunda Division RFEF Group 1", "Spain Segunda Division RFEF Group 2", "Spain Segunda Division RFEF Group 3", "Spain Segunda Division RFEF Group 4", "Spain Segunda Division RFEF Group 5", "Spain Primera Division RFEF Group 1", "Spain Primera Division RFEF Group 2", "Italy Cup", "Italy Serie A", "Italy Serie B", "Netherlands Eredivisie", "Netherlands Eerste Divisie", "Netherlands Cup", "USA MLS", "USA MLS Next Pro League", "USA USL", "UEFA Champions League", "UEFA Champions League Women", "UEFA U21 European Championship", "UEFA Europa League", "UEFA Conference League", "UEFA Nations League", "UEFA EURO Qualifiers", "CONCACAF Nations League", "CONCACAF U20 Championship Women", "CONCACAF Champions League", "FIFA U20 World Cup", "FIFA World Cup Women", "Belgium Cup", "Belgium Jupiler League", "Scotland Premier League", "Scotland Championship", "Scotland Cup", "Scotland Challenge Cup", "Scotland League 1", "Scotland League 2", "Turkey SuperLiga", "Turkey TFF 1 Lig", "Turkey TFF 2 Lig", "Switzerland SuperLeague", "Switzerland Challenge League", "Denmark SuperLiga", "Denmark Cup", "Denmark 1st Division", "Denmark 2nd Division", "Denmark U21", "Denmark 2nd Division Women", "Greece SuperLeague", "Greece SuperLeague 2", "Portugal Primeira Liga", "Portugal Segunda Liga", "Portugal Cup", "Portugal Cup U23", "Ireland Premier League", "Ireland Division 1", "Ireland Leinster Senior League", "Russia Premier League", "Russia Cup", "Austria 2 Liga", "Austria Bundesliga", "Austria Landesliga", "Austria Regionalliga Salzburg", "Austria Cup", "Austria KFV Cup ", "Australia A League", "Australia NPL Queensland", "Australia NPL Victorian", "Australia NPL Tasmania", "Australia NPL South Australia", "Australia NPL Western Australia", "Australia NPL New South Wales", "Australia NPL Capital Football", "Argentina Liga Pro", "Argentina Cup", "Argentina Primera Division Women", "Argentina Reserve League", "Argentina Primera B Nacional", "Argentina Primera B Metropolitana", "Argentina Primera C Metropolitana", "Argentina Primera D Metropolitana", "Argentina Torneo Federal A", "Japan J League", "Japan J League Division 2", "Brazil Serie A", "Brazil Serie B", "Brazil Serie C", "Brazil Serie D", "Brazil Copa Do Nordeste", "Brazil Cup", "Brazil Goiano U20", "Brazil Pernambucano U20", "Bahrain Premier League", "Bulgaria Parva Liga", "Bulgaria Parva Liga B", "Chile Primera Division", "Chile Primera B", "China Super League", "Colombia Primera A", "Colombia Primera B", "Colombia Cup", "Colombia Liga Women", "Cyprus First Division", "Czech Liga 1", "Czech U19 League", "Ecuador Serie A", "Ecuador Serie B", "Egypt Premiership", "Ethiopia Premier League", "Finland Ykkonen", "Finland Veikkausliiga", "Finland Kakkonen", "Iceland Urvalsdeild", "Iceland U19 League", "Iraq Premier League", "Mexico Primera Division", "Mexico Liga MX Women", "Norway Eliteserien", "Norway First Division", "Norway Second Division", "Norway Third Division Group 1", "Norway Third Division Group 2", "Norway Third Division Group 3", "Norway Third Division Group 4", "Norway Third Division Group 5", "Norway Third Division Group 6", "Poland Ekstraklasa", "Poland Liga 1", "Qatar Stars League", "SaudiArabia Premier League", "SaudiArabia Division 1", "SouthKorea K League 1", "SouthKorea K League 2", "SouthAfrica Premier League", "Sweden Allsvenskan", "Sweden Allsvenskan Women", "Sweden Cup", "Sweden Superettan", "Sweden Division 1", "Sweden Division 2", "Ukraine Premier League", "Thailand League 1", "Bolivia Liga Profesional", "Estonia Meistriliiga", "Iran Persian Gulf Pro League", "Latvia Virsliga", "Lithuania A Lyga", "Paraguay Primera Division", "Peru Primera Division", "Romania Liga 1", "Malaysia Super League", "Slovakia Super Liga", "Slovakia Liga 3", "Uruguay Primera Division", "Uruguay Reserve League", "Venezuela Primera Division", "Singapore Premier League", "UAEmirates Arabian Gulf League", "Croatia HNL", "Andorra Premier Division", "Armenia Premier League", "Georgia Erovnuli Liga", "Oman Professional League", "Kuwait Premier League", "Canada Premier League", "Hungary NB I", "Vietnam V.League 1", "Algeria Ligue 1", "Algeria Ligue 1 U21", "Faroe Premier League", "Jamaica Premier League", "Mauritania League 1", "Morocco Botola Pro", "Uzbekistan Super League"]

value_bet_finder = ValueBetFinder(
                        paired_collections=paired_collections, 
                        value_bet_collection=football_value_bet_collection,
                        logger=logger,
                        thread_pool_workers=8,
                        line=0,
                        competitions=football_competitions,
                        date_range=3
                        )

if __name__ == "__main__":
    
    while True:
        
        try:
            
            value_bets_found = value_bet_finder.find_value_bets_and_update_db()
            logger.info(f"{value_bets_found} value bets for upcoming football matches were found and added to db")
            
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"An exception of type {type(e).__name__} occurred while finding value bets: {str(e)}")