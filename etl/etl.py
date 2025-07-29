import requests
import pandas as pd
from sqlalchemy import create_engine, text, Integer, String, Text, Numeric, BigInteger
from typing import List, Dict


# STEP 1
#
# CONSTANTS
#
# according to the API documentation, thouth, we should limit our ALL request with up to 10 fields
# countries, flags and population are required, others I chose based on potential use cases
FIELDS = ["ccn3", "name", "flags", "population", "capital", "region", "languages", "area", "currencies", "gini"]

# use ALL endpoint to request all information with selected fields
URL = f"https://restcountries.com/v3.1/all?fields={','.join(FIELDS)}"

# fetching data from API
def fetch_countries_data() -> List[Dict]:
    """
    Extract
    Fetching 10 selected fields from countries API

    Returns:
        List of dicts by countries
    """
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as error:
        raise SystemExit(f"Fetching API's data ERROR: {error}")
    

# API's countries data response normalization
def normalize_countries_data(raw_countries_data: List[Dict]) -> Dict[str, pd.DataFrame]:
    """
    Transform
    Transforms JSON data into pandas DataFrames

    Returns:
        Dict of pandas DataFrames:
    """

    # using this 10 fields request we can get columns:
    #
    # common name, official name
    # currency name, currency symbol, 
    # capital, region, subregion, area, flag, population, gini in given year

    # Let's use Star schema for this data and create next tables:
    # countries, currencies, langueages, country_language, country_currency
    #
    # Countries schema:
    # country_code:    int
    # common_name:     String,
    # official_name:   String,
    # capital:         String,
    # region:          String,
    # flag_url:        String,
    # area:            float,
    # population       int
    # gini:            float
    #
    # Currencies schema:
    # currency_code:   String
    # currency_name:   String
    # currency_symbol: String
    #
    # Languages schema:
    # language_code:   String
    # language_name:   String
    #
    # Countries_currencies schema:
    # country_code:    int
    # currency_code:   String
    #
    # Countries_languages schema:
    # country_code:    int
    # language_code:   String

    # lets try to normalize this data first
    normalized_countries = []

    normalized_currencies = {}
    countries_currencies = []

    normalized_languages = {}
    countries_languages = []


    # cheack for doubles
    country_codes = set()
    
    for country in raw_countries_data:
        code = country.get("ccn3")
        if code in country_codes:
            print("found a double")
            continue
        country_codes.add(code)    
        
        try:
            # Skip if country code is missing or invalid
            ccn3 = country.get("ccn3")
            if not ccn3 or not str(ccn3).isdigit():
                continue
            country_code = int(ccn3)
            

            # handle gini
            gini_data = country.get("gini", {})
            gini_value = list(gini_data.values())[0] if gini_data else None

            # handle capitals. in case if some countries have none
            # for simplification lets ignore rare cases with multiple capitals
            capital_data = country.get("capital")
            capital = capital_data[0] if capital_data and len(capital_data) > 0 else None

            # creates country row
            normalized_country = {
                "country_code":          country_code,
                "country_name":          country.get("name", {}).get("common"),
                "official_country_name": country.get("name", {}).get("official"),
                "capital":               capital,                      
                "region":                country.get("region"),
                "flag_url":              country.get("flags", {}).get("png"),
                "area":                  country.get("area"),
                "population":            country.get("population"),
                "gini":                  gini_value
            }
            normalized_countries.append(normalized_country)

            # creates currency row with unique values
            currencies = country.get("currencies", {})
            for cur_code, cur_data in currencies.items():
                if isinstance(cur_data, dict):
                    if cur_code not in normalized_currencies:
                        normalized_currencies[cur_code] = {
                            "currency_code":   cur_code,
                            "currency_name":   cur_data.get("name"),
                            "currency_symbol": cur_data.get("symbol")
                        }
                    # creates junction country_currency row
                    countries_currencies.append({
                        "country_code":  country_code,
                        "currency_code": cur_code
                    })
            
            # creates languages row with unique values
            languages = country.get("languages", {})
            for lan_code, lan_name in languages.items():
                if isinstance(lan_name, str):
                    if lan_code not in normalized_languages:
                        normalized_languages[lan_code] = {
                            "language_code": lan_code,
                            "language_name": lan_name
                        }
                    # creates junction country_language row
                    countries_languages.append({
                        "country_code": country_code,
                        "language_code": lan_code
                    })
        except Exception as error:
            print(f"Error while normalizing country: {error}. Proccessing to the next one.")
            print(f"Error from country: {country.get('name', {}).get('common')}")
            continue
            
    normalized_currencies = list(normalized_currencies.values())
    normalized_languages = list(normalized_languages.values())

    return {
        "countries":            pd.DataFrame(normalized_countries),
        "currencies":           pd.DataFrame(normalized_currencies),
        "languages":            pd.DataFrame(normalized_languages),
        "countries_currencies": pd.DataFrame(countries_currencies),
        "countries_languages":  pd.DataFrame(countries_languages)
    }


# STEP 2
def run_etl():
    # load normalized data from the first step
    fetched_data = fetch_countries_data()
    normalized_dataframes = normalize_countries_data(fetched_data)

    # Connect to PostgreSQL
    DB_URL = "postgresql://admin:admin@localhost:5432/countries_db"
    engine = create_engine(DB_URL)

    # Save normalized DataFrames into the databases tables
    #
    # Clear existing tables with cascade to avoid dependency errors
    with engine.begin() as con:
        con.execute(text("DROP TABLE IF EXISTS countries_languages CASCADE;"))
        con.execute(text("DROP TABLE IF EXISTS countries_currencies CASCADE;"))
        con.execute(text("DROP TABLE IF EXISTS countries CASCADE;"))
        con.execute(text("DROP TABLE IF EXISTS currencies CASCADE;"))
        con.execute(text("DROP TABLE IF EXISTS languages CASCADE;"))
        con.execute(text("DROP VIEW IF EXISTS countries_summary;"))

    # MAIN tables first
    # Countries table
    normalized_dataframes["countries"].to_sql(
        "countries",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "country_code":          Integer(),
            "country_name":          String(100),
            "official_country_name": Text(),
            "capital":               String(100),
            "region":                String(50),
            "flag_url":              Text(),
            "area":                  Numeric(12, 2),
            "population":            BigInteger(),
            "gini":                  Numeric(4, 2)
        }
    )

    # currencies table
    normalized_dataframes["currencies"].to_sql(
        "currencies",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "currency_code":   String(3),
            "currency_name":   String(50),
            "currency_symbol": String(15)
        }
    )

    # languages table
    normalized_dataframes["languages"].to_sql(
        "languages",
        engine,
        if_exists="replace",
        index=False,
        dtype={
            "language_code": String(3),
            "language_name": String(50)
        }
    )

    # Now JUNCTION tables
    # Countries_currencies table
    normalized_dataframes["countries_currencies"].to_sql(
        "countries_currencies",
        engine,
        if_exists="replace",
        index=False
    )

    # Countries_languages table
    normalized_dataframes["countries_languages"].to_sql(
        "countries_languages",
        engine,
        if_exists="replace",
        index=False
    )

    with engine.begin() as con:
        # PRIMARY KEYS
        con.execute(text("ALTER TABLE countries ADD PRIMARY KEY (country_code);"))
        con.execute(text("ALTER TABLE currencies ADD PRIMARY KEY (currency_code);"))
        con.execute(text("ALTER TABLE languages ADD PRIMARY KEY (language_code);"))

        # primary keys for junction tables
        con.execute(text("""
            ALTER TABLE countries_currencies
            ADD PRIMARY KEY (country_code, currency_code);
        """))
        con.execute(text("""
            ALTER TABLE countries_languages
            ADD PRIMARY KEY (country_code, language_code);
        """))

        # add foreign keys countries_currencies
        con.execute(text("""
            ALTER TABLE countries_currencies
            ADD CONSTRAINT fk_countries FOREIGN KEY (country_code) REFERENCES countries(country_code) ON DELETE CASCADE,
            ADD CONSTRAINT fk_currencies FOREIGN KEY (currency_code) REFERENCES currencies(currency_code) ON DELETE CASCADE;
        """))

        # add foreign keys countries_languages
        con.execute(text("""
            ALTER TABLE countries_languages
            ADD CONSTRAINT fk_countries FOREIGN KEY (country_code) REFERENCES countries(country_code) ON DELETE CASCADE,
            ADD CONSTRAINT fk_languages  FOREIGN KEY (language_code) REFERENCES languages(language_code) ON DELETE CASCADE;
        """))

            # add constrains
        con.execute(text("""
            ALTER TABLE countries
            ADD CONSTRAINT chk_area_positive CHECK (area > 0),
            ADD CONSTRAINT chk_population_positive CHECK (population >= 0),
            ADD CONSTRAINT chk_gini_range CHECK (gini IS NULL OR (gini BETWEEN 0 AND 100));
        """))

        # create indexes
        con.execute(text("""
            CREATE INDEX idx_countries_region ON countries(region);
            CREATE INDEX idx_countries_population ON countries(population);
        """))

        # create a data mart for visualization
        con.execute(text("""
            CREATE OR REPLACE VIEW countries_summary AS
            SELECT
                c.country_name          AS "Country Name",
                c.official_country_name AS "Official Country Name",
                CASE
                    WHEN c.capital IS NULL
                        THEN 'N/A'
                    ELSE c.capital
                END                     AS "Capital",
                c.region                AS "Region",
                c.area                  AS "Area",
                c.population            AS "Population",
                c.gini                  AS "GINI",
                c.flag_url,
                CASE
                    WHEN COUNT(cr.currency_name)=0
                        THEN 'N/A'
                    ELSE STRING_AGG(DISTINCT cr.currency_name, ', ')
                END AS "Currencies",
                CASE
                    WHEN COUNT(l.language_name)=0
                        THEN 'N/A'
                    ELSE STRING_AGG(DISTINCT l.language_name, ', ') 
                END AS "Languages"
            FROM countries c
            
            LEFT JOIN countries_currencies cc USING(country_code)
            LEFT JOIN currencies cr USING(currency_code)
            LEFT JOIN countries_languages cl USING(country_code)
            LEFT JOIN languages l USING(language_code)

            GROUP BY
                c.country_name, c.official_country_name, c.capital, c.region, c.area, c.population, c.gini, c.flag_url
        """))

if __name__ == "__main__":
    run_etl()