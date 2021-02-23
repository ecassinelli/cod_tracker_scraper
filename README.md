# COD Tracker Web Scraper
Script to get player data from the website http://cod.trackker.gg for
the game Call of Duty Warzone/

The script takes a players_info.csv dataset, that contains information of several players, as an input, extracts and transforms the data and loads it into the SQL databse. 

BONUS: simple dinamic Plotly dashboard to visualize barplots to compare players in each stat. 

## Contents

```bash
.
├── cod_dashboard.py
├── cod_tracker.py
├── datasets
│   ├── players_info.csv
│   └── players_stats.csv
└── README.md
```