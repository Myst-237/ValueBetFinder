from typing import Tuple, Callable, List, Union
from datetime import datetime
import hashlib
from thefuzz import fuzz

def equalize_matches(match1: dict, match2: dict) -> Tuple[dict, dict]:
    """
    Description
    -----------
    Given two matches, filter them to keep only the fields where the keys are equal.
    This function handles nested dictionaries and lists.
    
    Parameters
    ----------
    - match1: dict
    - match2: dict
    
    Returns
    --------
    - Tuple[dict, dict]: A tuple containing the equalized matches
    """
    
    # Get the keys that are common to both dictionaries
    common_keys = set(match1.keys()) & set(match2.keys())

    # Create new dictionaries with the common keys and their values
    filteredmatch1 = {}
    filteredmatch2 = {}
    for key in common_keys:
        value1 = match1[key]
        value2 = match2[key]

        # Handle nested dictionaries recursively
        if isinstance(value1, dict) and isinstance(value2, dict):
            nestedmatch1, nestedmatch2 = equalize_matches(value1, value2)
            filteredmatch1[key] = nestedmatch1
            filteredmatch2[key] = nestedmatch2
        else:
            # Handle lists and other types
            filteredmatch1[key] = value1
            filteredmatch2[key] = value2

    return filteredmatch1, filteredmatch2

def market_odds_to_probabilities(market_odds: list) -> list:
    """
    Description
    ----------
    Converts a list of market odds to probabilities
    
    Parameters
    ----------
    - market_odds: list
        The market odds to be converted to probabilities
        
    Returns
    ------ 
    - list: List of the market with the probabilities of the conditions
    """
    
    market_probabilities = []
    
    for odd in market_odds:
        market_probabilities.append(1/odd)
        
    return market_probabilities

def no_vig_odds(market_odds: list) -> list:
    """
    Description
    ----------
    Converts a list of market odds to no vig odds
    
    Parameters
    ----------
    - market_odds: list
        The market odds for which the vig is to be removed
        
    Returns
    ------ 
    - list: List of the market odds with the vig removed
    """
    
    no_vig_odds = []
    market_probabilities = market_odds_to_probabilities(market_odds)
    sum_market_probabilities = sum(market_probabilities)
    
    if len(market_probabilities) == 1:
        no_vig_odds.append(1/market_probabilities[0])
    else:
        for prob in market_probabilities:
            no_vig_odds.append(sum_market_probabilities/prob)
        
    return no_vig_odds

def update_match_scoreboard(match: dict, update_function: Callable[[list], list]) -> dict:
    """ 
    Description
    ---------
    Updates a match using an update function
    
    Parameters
    ----------
    - match: dict
        The match to be updated
    - update_function: Callable[[list], list]
        The function used to update the match
        
    Returns
    --------
    dict: The updated match
    """
    updated_match = {}
    
    for k, v in match.items():
        if isinstance(v, list):
            try:
                updated_match[k] = update_function(v)
            except TypeError:
                updated_match[k] = v
        elif isinstance(v, dict):
            updated_match[k] = update_match_scoreboard(v, update_function)
        else:
            updated_match[k] = v
            
    return updated_match

def compare_market(market1: list, market2: list, bookmaker1: str, bookmaker2: str) -> list:
    """ 
    Description
    -----------
    Given two markets,  this function compares them to determine whether there is a value bet
    
    Parameters
    ---------- 
    - market1: list 
        The first market
    - market2: list 
        The second market
        
    Returns
    ----------
    - list: A list of all the possible value bets
    
    Raises
    ------
    - AttributeError: if the two markets are not of thesame length
    """
    if len(market1) == len(market2):
        value_bets = []
        for i in range(0, len(market1)):
            if market2[i] >= market1[i]+0.02 and 1.7 <= market2[i] and market1[i] <= 2.3 and (((market2[i]/market1[i]) - 1)*100) <= 12:
                value_bets.append({"Condition": str(i), bookmaker1: market1[i], bookmaker2: market2[i], "ROI": str(((market2[i]/market1[i]) - 1)*100)+'%'})
        return value_bets
    else:
        raise AttributeError('compare_market, market1 and market2 should have thesame length')

def add_timestamps(self, doc: dict) -> dict:
    """adds the time and date modified and created"""
    now = datetime.utcnow()
    date = now.strftime("%d/%m/%y")
    time = now.strftime("%H:%M:%S")
    doc['created_date'] = date
    doc['created_time'] = time
    return doc

def compare_markets(match1: dict, match2: dict, bookmaker1: str, bookmaker2: str) -> dict:
    """
    Description
    ------------
    Given two matches, compares them and determines if there is any market with a value bet
    
    Parameters
    ----------- 
    - match1: dict
        The first match
    - match2: dict
        The second match
    - bookmaker1: str
        The name of the bookmaker offering match1
    - bookmaker2: str
        The name of the bookmaker offering match2
        
    Returns:
    - dict: the final match after comparison, representing the value bets
    """
    final_match = {}
    
    
    if "_id" in match1.keys():
        final_match["_id"] = hashlib.md5(( match1["_id"]+match1["bookmaker_name"] + match2["bookmaker_name"]).encode()).hexdigest()
        final_match["bookmaker_name"] = match1["bookmaker_name"] + " - "+ match2["bookmaker_name"]
        final_match["match_id"] = match1["_id"] + ' - ' + match2["_id"]
        final_match["teams"] = str(match1["teams"]) + ' - ' + str(match2["teams"])
        final_match["sport_name"] = match1["sport_name"]
        final_match["competition"] = match1["competition"]
        final_match["date"] = f'{match1["date"]} | {match2["date"]}'
        final_match["time"] = f'{match1["time"]} | {match2["time"]}'
        final_match["is_live"] = match1["is_live"]
        now = datetime.utcnow()
        date = now.strftime("%d/%m/%y")
        time = now.strftime("%H:%M:%S")
        final_match['created_date'] = date
        final_match['created_time'] = time
        time_format = '%H:%M:%S'
        
        try:
            match1_last_modified_time = datetime.strptime(match1['last_modified_time'], time_format).time()
            match2_last_modified_time = datetime.strptime(match2['last_modified_time'], time_format).time()
            if match1_last_modified_time > match2_last_modified_time:
                final_match['value_bet_age'] = match2_last_modified_time.strftime("%H:%M:%S")
            else:
                final_match['value_bet_age'] = match1_last_modified_time.strftime("%H:%M:%S")
        except KeyError:
            pass
        
    for k, v in match1.items():
        
        if isinstance(v, list):
            
            if (k == "teams" or k == "HT-FT"):
                pass
            else:
                if len(v) == len(match2[k]):
                    final_match[k] = compare_market(v, match2[k], bookmaker1, bookmaker2)
                else:
                    final_match[k] = False
                    
        elif isinstance(v, dict):
            if k != "Correct Score":
                final_match[k] = compare_markets(v, match2[k], bookmaker1, bookmaker2)

    return final_match

def remove_false_fields(input_dict: dict) -> dict:
    """
    Recursively removes false fields from a given dictionary or list
    """
    if isinstance(input_dict, dict):
        return {
            k: remove_false_fields(v) for k, v in input_dict.items()
            if v not in [None, {}, [], False] and remove_false_fields(v) not in [None, {}, [], False]
        }
    elif isinstance(input_dict, list):
        return [
            remove_false_fields(market) for market in input_dict
            if market not in [None, {}, [], False] and remove_false_fields(market) not in [None, {}, [], False]
        ]
    else:
        return input_dict

def find_similar_football_match(match_list: list, match: dict) -> dict:
    """
    Find the most similar match from a list of football matches

    Args:
    - match_list (list): A list of dict representing the football matches.
    - match (dict): the target football match

    Returns:
    - dict: The most similar football match
    """
    best_match = None
    best_ratio = 0
    
    for _match in match_list:
        
        ratio = fuzz.token_sort_ratio((_match["teams"][0]+' vs '+_match["teams"][1]).lower(), (match["teams"][0]+' vs '+match["teams"][1]).lower())
        if ratio > best_ratio:
            best_ratio = ratio
            best_match = _match
        if best_ratio >= 99:
            break
    
    return best_match