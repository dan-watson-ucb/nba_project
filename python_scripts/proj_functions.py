import io
import os
import psycopg2 as pg
from psycopg2 import Error
from sqlalchemy import create_engine
import pandas as pd
import datetime as dt
from connection_data import c_data 
import nba_api.stats.endpoints as ep

def get_last_date():
    con = pg.connect(user = c_data['user'],
                                  password = c_data['password'],
                                  host = c_data['host'],
                                  port = c_data['port'],
                                  database = c_data['database'])
    query = "SELECT * from raw.last_pull"
    return pd.read_sql(query, con)['last_pull']
    con.close()
    

def update_last_date(new_date):
    con = pg.connect(user = c_data['user'],
                                  password = c_data['password'],
                                  host = c_data['host'],
                                  port = c_data['port'],
                                  database = c_data['database'])
    query = 'UPDATE raw.last_pull SET last_pull = %s WHERE id = 1'
    try:
        cur = con.cursor()
        cur.execute(query, (new_date,))
        con.commit()
        cur.close()
    except (Exception, pg.DatabaseError) as error:
        print(error)
    finally:
        if con is not None:
            con.close()
            
            
def get_scoreboard(date):
    sb = ep.scoreboardv2.ScoreboardV2(game_date = date).get_data_frames()[0]
    if len(sb)== 0:
        return "no games"
    else:
        return sb


    
def get_player_bs_advanced(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            player_bs_advanced = ep.boxscoreadvancedv2.BoxScoreAdvancedV2(gid).get_data_frames()[0]
        else:
            player_bs_advanced = player_bs_advanced.append(ep.boxscoreadvancedv2.BoxScoreAdvancedV2(gid).get_data_frames()[0])
    
    return player_bs_advanced


def get_team_bs_advanced(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            team_bs_advanced = ep.boxscoreadvancedv2.BoxScoreAdvancedV2(gid).get_data_frames()[1]
        else:
            team_bs_advanced = player_bs_advanced.append(ep.boxscoreadvancedv2.BoxScoreAdvancedV2(gid).get_data_frames()[1])
    
    return team_bs_advanced



def get_player_bs_ff(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            player_bs_ff = ep.boxscorefourfactorsv2.BoxScoreFourFactorsV2(gid).get_data_frames()[0]
        else:
            player_bs_ff = player_bs_ff.append(ep.boxscorefourfactorsv2.BoxScoreFourFactorsV2(gid).get_data_frames()[0])
    
    return player_bss_ff 

def get_team_bs_ff(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            team_bs_ff = ep.boxscorefourfactorsv2.BoxScoreFourFactorsV2(gid).get_data_frames()[1]
        else:
            team_bs_ff = team_bs_ff.append(ep.boxscorefourfactorsv2.BoxScoreFourFactorsV2(gid).get_data_frames()[1])
            
    return team_bs_ff



def get_player_bs_misc(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            player_bs_misc = ep.boxscoremiscv2.BoxScoreMiscV2(gid).get_data_frames()[0]
        else:
            player_bs_misc = player_bs_misc.append(ep.boxscoremiscv2.BoxScoreMiscV2(gid).get_data_frames()[0])
    
    return player_bs_misc

def get_team_bs_misc(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            team_bs_misc = ep.boxscoremiscv2.BoxScoreMiscV2(gid).get_data_frames()[1]
        else:
            team_bs_misc = team_bs_misc.append(ep.boxscoremiscv2.BoxScoreMiscV2(gid).get_data_frames()[1])
            
    return team_bs_misc


            
def get_player_bs_pt(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            player_bs_pt = ep.boxscoreplayertrackv2.BoxScorePlayerTrackV2(gid).get_data_frames()[0]
        else:
            player_bs_pt = player_bs_pt.append(ep.boxscoreplayertrackv2.BoxScorePlayerTrackV2(gid).get_data_frames()[0])
    
    return player_bs_pt

def get_team_bs_pt(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            team_bs_pt = ep.boxscoreplayertrackv2.BoxScorePlayerTrackV2(gid).get_data_frames()[1]
        else:
            team_bs_pt = team_bs_pt.append(ep.boxscoreplayertrackv2.BoxScorePlayerTrackV2(gid).get_data_frames()[1])
            
    return team_bs_pt



def get_player_bs_scoring(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            player_bs_scoring = ep.boxscorescoringv2.BoxScoreScoringV2(gid).get_data_frames()[0]
        else:
            player_bs_scoring = player_bs_scoring.append(ep.boxscorescoringv2.BoxScoreScoringV2(gid).get_data_frames()[0])
    
    return player_bs_scoring

def get_team_bs_scoring(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            team_bs_scoring = ep.boxscorescoringv2.BoxScoreScoringV2(gid).get_data_frames()[1]
        else:
            team_bs_scoring = team_bs_scoring.append(ep.boxscorescoringv2.BoxScoreScoringV2(gid).get_data_frames()[1])

    return team_bs_scoring



def get_player_bs_summ(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            player_bs_summ = ep.boxscoresummaryv2.BoxScoreSummaryV2(gid).get_data_frames()[0]
        else:
            player_bs_summ = player_bs_summ.append(ep.boxscoresummaryv2.BoxScoreSummaryV2(gid).get_data_frames()[0])
    
    return player_bs_summ

def get_team_bs_summ(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            team_bs_summ = ep.boxscoresummaryv2.BoxScoreSummaryV2(gid).get_data_frames()[1]
        else:
            team_bs_summ = player_bs_summ.append(ep.boxscoresummaryv2.BoxScoreSummaryV2(gid).get_data_frames()[1])
    
    return team_bs_summ



def get_player_bs_trad(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            player_bs_trad = ep.boxscoretraditionalv2.BoxScoreTraditionalV2(gid).get_data_frames()[0]
        else:
            player_bs_trad = player_bs_trad.append(ep.boxscoretraditionalv2.BoxScoreTraditionalV2(gid).get_data_frames()[0])
        
    return player_bs_trad

def get_team_bs_trad(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            team_bs_trad = ep.boxscoretraditionalv2.BoxScoreTraditionalV2(gid).get_data_frames()[1]
        else:
            team_bs_trad = team_bs_trad.append(ep.boxscoretraditionalv2.BoxScoreTraditionalV2(gid).get_data_frames()[1])
            
    return team_bs_trad


def get_player_bs_tracking(gids):
	for j, gid in enumerate(gids):
		if j == 0:
			player_bs_tracking = ep.BoxScorePlayerTrackV2(gid).get_data_frames()[0]
		else:
			player_bs_tracking = player_bs_tracking.append(ep.BoxScorePlayerTrackV2(gid).get_data_frames()[0])

	return player_bs_tracking

def get_team_bs_tracking(gids):
	for j, gid in enumerate(gids):
		if j == 0:
			team_bs_tracking = ep.BoxScorePlayerTrackV2(gid).get_data_frames()[1]
		else:
			team_bs_tracking = player_bs_tracking.append(ep.BoxScorePlayerTrackV2(gid).get_data_frames()[1])

	return team_bs_tracking




def get_player_bs_usage(gids):
    for j, gid in enumerate(gids):
        if j == 0:
            player_bs_usage = ep.boxscoreusagev2.BoxScoreUsageV2(gid).get_data_frames()[0]
        else:
            player_bs_usage = player_bs_usage.append(ep.boxscoreusagev2.BoxScoreUsageV2(gid).get_data_frames()[0])
    
    return player_bs_usage

         
    
def save_data(df, name, location):
    os.chdir(location)
    df.to_csv(name)
    print('{} saved to {}'.format(name, location))


def upload_data(df, table_name):

    engine = create_engine('postgresql+pg://{}:{}@{}:{}/{}'.format(c_data['user'],\
                                                                                        c_data['password'],\
                                                                                        c_data['host'],\
                                                                                        c_data['port'],\
                                                                                        c_data['database']))

    df.to_sql(table_name, engine, if_exists='append',index=False)

    