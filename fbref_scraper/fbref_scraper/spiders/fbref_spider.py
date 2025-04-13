import scrapy
import json
import time
import logging

class FBrefTableSpider(scrapy.Spider):
    name = "league_table"
    
    def start_requests(self):
        url = "https://fbref.com/en/comps/9/Premier-League-Stats"
        headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36", 
        "Referer": "https://google.com",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br"
    }
        yield scrapy.Request(url, headers=headers, callback=self.parse)
   
    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure}")
    
    def parse(self, response):
        """ Scrape the Premier League Table in JSON Format """

        # league metadata for clarity
        league_meta_info = response.css("h1::text").get()
        league_meta_info = league_meta_info.replace(" Stats", "").split(" ")
       
        country = response.xpath("//p[1]/a/text()").get()

        season = league_meta_info[0].strip()
        
        #check if league_name is a single word
        if len(league_meta_info) > 1:
            league_name = "_".join(league_meta_info[1:]).lower().strip()
        else: 
            league_name = league_meta_info[1].lower().strip()
        
        league_data = {
            "league": league_name,
            "country": country,
            "season": season,
            "teams": {}
        }
        
        tables = response.css("div.switcher_content table").getall()
        
        self.logger.info(f"Found {len(tables)} tables") 

        if not tables:
            self.logger.warning("No tables found! Check your CSS selector.")


        '''
        time.sleep(3)
        #Scrape team-level data
        for row in response.css("table.stats_table tbody tr"):
            team = row.css("td[data-stat='team'] a::text").get()

            if team is None:
                continue

            league_data["teams"][team] = {
                "team": team,
                "matches_played": int(row.css("td[data-stat='games']::text").get(default="0")),
                "wins": {"total": int(row.css("td[data-stat='wins']::text").get(default="0"))},
                "draws": {"total": int(row.css("td[data-stat='ties']::text").get(default="0"))},
                "losses": {"total": int(row.css("td[data-stat='losses']::text").get(default="0"))},
                "goals_for": {"total": int(row.css("td[data-stat='goals_for']::text").get(default="0"))},
                "goals_against": {"total": int(row.css("td[data-stat='goals_against']::text").get(default="0"))},
                "xG": {"total": float(row.css("td[data-stat='xg_for']::text").get(default="0"))},
                "xGA": {"total": float(row.css("td[data-stat='xg_against']::text").get(default="0"))}
            }

        for row in response.css("table.stats_table:nth-of-type(2) tbody tr"):
            team = row.css("td[data-stat='team'] a::text, th[data-stat='team'] a::text").get(default="N/A").strip()
            if team not in league_data["teams"]:
                continue  # Skip teams that were not found in the first table

            league_data["teams"][team]["wins"]["home"] = int(row.css("td[data-stat='home_wins']::text").get(default="0").strip())
            league_data["teams"][team]["wins"]["away"] = int(row.css("td[data-stat='away_wins']::text").get(default="0").strip())
            league_data["teams"][team]["draws"]["home"] = int(row.css("td[data-stat='home_draws']::text").get(default="0").strip())
            league_data["teams"][team]["draws"]["away"] = int(row.css("td[data-stat='away_draws']::text").get(default="0").strip())
            league_data["teams"][team]["losses"]["home"] = int(row.css("td[data-stat='home_losses']::text").get(default="0").strip())
            league_data["teams"][team]["losses"]["away"] = int(row.css("td[data-stat='away_losses']::text").get(default="0").strip())
            league_data["teams"][team]["goals_for"]["home"] = int(row.css("td[data-stat='home_goals_for']::text").get(default="0").strip())
            league_data["teams"][team]["goals_for"]["away"] = int(row.css("td[data-stat='away_goals_for']::text").get(default="0").strip())
            league_data["teams"][team]["goals_against"]["home"] = int(row.css("td[data-stat='home_goals_against']::text").get(default="0").strip())
            league_data["teams"][team]["goals_against"]["away"] = int(row.css("td[data-stat='away_goals_against']::text").get(default="0").strip())
            league_data["teams"][team]["xG"]["home"] = float(row.css("td[data-stat='home_xg_for']::text").get(default="0").strip())
            league_data["teams"][team]["xG"]["away"] = float(row.css("td[data-stat='away_xg_for']::text").get(default="0").strip())
            league_data["teams"][team]["xGA"]["home"] = float(row.css("td[data-stat='home_xg_against']::text").get(default="0").strip())
            league_data["teams"][team]["xGA"]["away"] = float(row.css("td[data-stat='away_xg_against']::text").get(default="0").strip())

           

        # Convert dictionary to list format for JSON output
        final_data = {
            "league": league_data["league"],
            "country": league_data["country"],
            "season": league_data["season"],
            "teams": list(league_data["teams"].values())
        }


        #Save JSON file
        filename = f"{league_name}_{season}.json"
        with open(filename,"w", encoding = "utf-8") as file:
            json.dump(final_data, file, ensure_ascii=False, indent=4 )

        self.log(f"Data saved to {filename}")
        '''
        

        

        


    