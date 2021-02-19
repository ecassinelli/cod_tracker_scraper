import requests, bs4, psycopg2, re, csv
import pandas as pd
from datetime import date
from psycopg2 import Error

stats_to_extract = ['player', 'wins', 'kd_ratio', 'kills', 'deaths', 'downs', 'damage', 'avg_life', 'top_weapon', 'date', 'level']

players_info_df = pd.read_csv('./datasets/players_info.csv', dtype={'player': str, 'account': str, 'acc_id': str})

def extract_export(source_dataset):
    players_stats_df = pd.DataFrame(columns=stats_to_extract)

    for index in source_dataset.index:
        player = players_info_df['player'][index]
        account = players_info_df['acc_id'][index]

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
        }

        player_regex = re.compile(r'(.+)#(.+)')

        if account == '1':
            response = requests.get('https://cod.tracker.gg/warzone/profile/psn/' + player + '/overview', headers=headers)
            if response.status_code != 200:
                print('Profile not found!')
                quit()
        elif account == '2':
            if player_regex.match(player) == None:
                print('Invalid username format.')
                quit()
            player_name = re.split('#', player)[0]
            player_num = re.split('#', player)[1]
            response = requests.get('https://cod.tracker.gg/warzone/profile/atvi/' + player_name + '%23'+ player_num +'/overview', headers=headers)
            if response.status_code != 200:
                print('Profile not found!')
                quit()
        elif account == '3':
            if player_regex.match(player) == None:
                print('Invalid username format.')
                quit()
            player_name = re.split('#', player)[0]
            player_num = re.split('#', player)[1]
            response = requests.get('https://cod.tracker.gg/warzone/profile/battlenet/' + player_name + '%23'+ player_num +'/overview', headers=headers)
            if response.status_code != 200:
                print('Profile not found!')
                quit()

        soup_web = bs4.BeautifulSoup(response.text, 'html.parser')

        level = soup_web.select('#app > div.trn-wrapper > div.trn-container > div > main > div.content.no-card-margin > div.site-container.trn-grid.trn-grid--vertical.trn-grid--small > div:nth-child(2) > div > div.trn-grid__sidebar-right > div > div > div.highlighted.highlighted--giants > div.highlighted-stat.highlighted-stat--progression > div > div.highlight-text')
        level_value = int(level[0].text.strip()[6:9])

        wins = soup_web.select('#app > div.trn-wrapper > div.trn-container > div > main > div.content.no-card-margin > div.site-container.trn-grid.trn-grid--vertical.trn-grid--small > div:nth-child(2) > div > div:nth-child(2) > div:nth-child(1) > div.main > div:nth-child(1) > div > div.numbers > span.value')
        wins_value = int(wins[0].text.strip())
        
        kdratio = soup_web.select('#app > div.trn-wrapper > div.trn-container > div > main > div.content.no-card-margin > div.site-container.trn-grid.trn-grid--vertical.trn-grid--small > div:nth-child(2) > div > div:nth-child(2) > div:nth-child(1) > div.main > div:nth-child(7) > div > div.numbers > span.value')
        kdratio_value = float(kdratio[0].text.strip())

        kills = soup_web.select('#app > div.trn-wrapper > div.trn-container > div > main > div.content.no-card-margin > div.site-container.trn-grid.trn-grid--vertical.trn-grid--small > div:nth-child(2) > div > div:nth-child(2) > div:nth-child(1) > div.main > div:nth-child(5) > div > div.numbers > span.value')
        kills_value = int(kills[0].text.strip().replace(',', ''))

        deaths = soup_web.select('#app > div.trn-wrapper > div.trn-container > div > main > div.content.no-card-margin > div.site-container.trn-grid.trn-grid--vertical.trn-grid--small > div:nth-child(2) > div > div:nth-child(2) > div:nth-child(1) > div.main > div:nth-child(6) > div > div.numbers > span.value')
        deaths_value = int(deaths[0].text.strip().replace(',', ''))

        downs = soup_web.select('#app > div.trn-wrapper > div.trn-container > div > main > div.content.no-card-margin > div.site-container.trn-grid.trn-grid--vertical.trn-grid--small > div:nth-child(2) > div > div:nth-child(2) > div:nth-child(1) > div.main > div:nth-child(8) > div > div.numbers > span.value')
        downs_value = int(downs[0].text.strip().replace(',', ''))

        damage_by_game = soup_web.select('#app > div.trn-wrapper > div.trn-container > div > main > div.content.no-card-margin > div.site-container.trn-grid.trn-grid--vertical.trn-grid--small > div:nth-child(2) > div > div.trn-grid__sidebar-right > div > div > div.giant-stats > div:nth-child(4) > div > div.numbers > span.value')
        damage_value = float(damage_by_game[0].text.strip().replace(',', ''))

        avg_life = soup_web.select('#app > div.trn-wrapper > div.trn-container > div > main > div.content.no-card-margin > div.site-container.trn-grid.trn-grid--vertical.trn-grid--small > div:nth-child(2) > div > div.trn-grid__sidebar-right > div > div > div.main > div:nth-child(6) > div > div.numbers > span.value')
        avg_life_value = avg_life[0].text.strip()

        top_weapon = soup_web.select('#app > div.trn-wrapper > div.trn-container > div > main > div.content.no-card-margin > div.site-container.trn-grid.trn-grid--vertical.trn-grid--small > div:nth-child(2) > div > div.trn-grid.trn-grid--two > div:nth-child(1) > div.segment-top__first > div.segment-top__content > div.segment-top__details > span')
        top_weapon_value = top_weapon[0].text.strip()

        player_info = {'player': player, 'wins': wins_value, 'kd_ratio': kdratio_value, 'kills': kills_value, 'deaths': deaths_value, 'downs': downs_value, 'damage': damage_value, 'avg_life': avg_life_value, 'top_weapon': top_weapon_value, 'date': date.today(),'level':  level_value}

        players_stats_df = players_stats_df.append(player_info, ignore_index=True)

    players_stats_df.to_csv('./datasets/players_stats.csv', index=False)

def load_dataset():
    try:
        connection = psycopg2.connect(user="postgres",
                                    password="4387638755**",
                                    host="127.0.0.1",
                                    port="5432",
                                    database="leaderboards")

        cursor = connection.cursor()

        create_table_query = """
        CREATE TABLE players_stats (
            player VARCHAR(100) PRIMARY KEY,
            wins BIGINT,
            kd_ratio NUMERIC,
            kills BIGINT,
            deaths BIGINT,
            downs BIGINT,
            damage NUMERIC,
            avg_life VARCHAR(50),
            top_weapon VARCHAR(50),
            date DATE,
            level SMALLINT 
        );
        """

        copy_query = """
        COPY players_stats FROM STDIN DELIMITER ',' CSV HEADER;
        """
        
        cursor.execute(create_table_query)

        with open('./datasets/players_stats.csv') as csv_file:
            cursor.copy_expert(copy_query, csv_file)
        
        connection.commit()

        print('Insertion to leaderbords database in PostgreSQL successful')

    except (Exception, Error) as error:
        if connection:
            print("Error while connecting to PostgreSQL", error)

    finally:
        if connection:
            cursor.close()
            connection.close()

def run_application():
    extract_export(players_info_df)
    load_dataset()

if __name__ == '__main__':
    run_application()
