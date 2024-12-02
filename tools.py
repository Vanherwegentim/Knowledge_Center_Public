import json

from django.utils import timezone
from llama_index.core.tools import FunctionTool
import datetime
from enums.account_type import AccountType
from utils import (
    get_acount_details_by_account_number,
    get_db_connection,
    get_period_ids,
)

# cursor = conn.cursor()


def multiply(a: float, b: float) -> float:
    """Multiply two numbers and returns the product"""
    return a * b


def add(a: float, b: float) -> float:
    """Add two numbers and returns the sum"""
    return a + b


def account_api_call(company_id: str, page: int = 1, page_size: int = 100):
    """
    Haalt een specifieke pagina van accountdossiers op voor een bedrijf, geïdentificeerd door de company_id.

    Vereist:
    - company_id (str): Het unieke ID van het bedrijf waarvoor de accounts moeten worden opgehaald.
    - page (int): De pagina van resultaten die moet worden opgehaald (standaard is pagina 1).
    - page_size (int): Het aantal accountdossiers per pagina (standaard is 100).

    Retourneert:
    - Een lijst met accountdossiers voor de opgegeven pagina, of een foutmelding als het bedrijf niet bestaat of geen accounts heeft.
    """
    with open("silverfin_api_static_db/accounts.json", "r") as file:
        accounts = json.load(file)

    # Get the accounts for the specified company
    company_accounts = accounts.get(
        str(company_id), "Geen accounts gevonden voor het opgegeven bedrijf."
    )

    if isinstance(company_accounts, str):  # If the result is the error message
        return company_accounts

    # Calculate the start and end indices for pagination
    start_index = (page - 1) * page_size
    end_index = start_index + page_size

    # Return the paginated results
    paginated_accounts = company_accounts[start_index:end_index]

    return (
        paginated_accounts
        if paginated_accounts
        else "Geen meer accounts voor deze pagina."
    )


def company_api_call(company_id: str):
    """
    Geeft informatie terug over het bedrijf met de gegeven ID
    Vereist:
    - company_id (int): Het unieke ID van het bedrijf waarvoor de gegevens moeten worden opgehaald.

    Retourneert:
    - Een dictionary met bedrijfsinformatie als het bedrijf wordt gevonden, of een foutmelding als het bedrijf niet bestaat.
    """
    with open("silverfin_api_static_db/companies.json", "r") as file:
        companies = json.load(file)

    if companies[company_id]:
        return companies[company_id]
    return "Geen bedrijf gevonden met de opgegeven company_id."


def companies_ids_api_call(keywords: list = None):
    """
    Geeft de bedrijfs-ids met de overeenkomstige naam terug. Gebruik deze tool wanneer je de company_id niet weet.
    Vereist:
    - keywords: Een lijst met zoekwoorden om te filteren op bedrijfsnamen
    Retourneert:
    - Een lijst met de ids van alle bedrijven voor de opgegeven pagina, gefilterd op zoekwoorden.
    """
    companies = """
        SELECT company_id, name
        FROM companies
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(companies)
            result = cursor.fetchall()

    # Filter results based on keywords
    if keywords:
        filtered_result = [
            (company_id, name)
            for company_id, name in result
            if any(keyword.lower() in name.lower() for keyword in keywords)
        ]
    else:
        filtered_result = result

    return filtered_result


def period_api_call(company_id: int, page: int = 1, page_size: int = 100):
    """
    Haalt een specifieke pagina van periodes op voor een bedrijf, geïdentificeerd door de company_id.

    Vereist:
    - company_id (int): Het unieke ID van het bedrijf waarvoor de periodes moeten worden opgehaald.
    - page (int): De pagina van resultaten die moet worden opgehaald (standaard is pagina 1).
    - page_size (int): Het aantal periodes per pagina (standaard is 100).

    Retourneert:
    - Een lijst met periodes die horen bij het opgegeven bedrijf voor de opgegeven pagina.
    """
    with open("silverfin_api_static_db/periods.json", "r") as file:
        periods = json.load(file)

    # Get the periods for the specified company
    company_periods = periods.get(
        str(company_id), "Geen periodes gevonden voor het opgegeven bedrijf."
    )

    if isinstance(company_periods, str):  # If the result is an error message
        return company_periods

    # Calculate the start and end indices for pagination
    start_index = (page - 1) * page_size
    end_index = start_index + page_size

    # Return the paginated results
    paginated_periods = company_periods[start_index:end_index]

    return (
        paginated_periods
        if paginated_periods
        else "Geen meer periodes voor deze pagina."
    )


def company_id_to_name_converter(company_id: int):
    with open("silverfin_api_static_db/company_ids.json", "r") as file:
        companies = json.load(file)
        return companies[str(company_id)]


def has_tax_decreased_api_call(company_id: int, date: str):
    """
    Geeft de SQL-query terug om het belastingpercentage op te halen voor een bedrijf op een specifieke datum. Als het 20% dan is het een verlaagd tarief. Als het 25% is dan is het een normaal tarief.
    Args:
        company_id (int): Unieke identificatie van het bedrijf, bijv. 12345.
        date (str): Datum in "YYYY-MM-DD"-formaat, bijv. "2023-12-31".
    Returns:
        float: Het belastingpercentage voor het bedrijf op de opgegeven datum.
    Raises:
        ValueError: Als company_id of date ontbreekt of ongeldig is.
    """

    if not company_id:
        raise ValueError("company_id is vereist en mag niet leeg zijn.")
    if not date:
        raise ValueError("date is vereist en moet in het 'YYYY-MM-DD'-formaat zijn.")

    with get_db_connection() as conn:
        with conn.cursor() as cursor: 
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):
                return period_id
    return f"""SELECT tax_percentage
              FROM reconciliation_results
              WHERE company_id = {company_id} and period_id = {period_id}"""
    # with get_db_connection() as conn:
    #     with conn.cursor() as cursor:
    #         period_id = get_period_ids(cursor, company_id, date)
    #         if isinstance(period_id, str):
    #             return period_id
    #         sql = f"""SELECT tax_percentage
    #           FROM reconciliation_results
    #           WHERE company_id = {company_id} and period_id = {period_id}"""
    #         cursor.execute(sql)
    #         tax_percentage = cursor.fetchall()
    #         if len(tax_percentage) == 0:
    #             return "Er is geen reconciliation voor deze periode"
    # return tax_percentage


def get_date():
    """
    A function to return todays date.
    Call this before any other functions if you are unaware of the current date
    """
    return datetime.date.today()


def period_id_fetcher(date: str, company_id: int):
    """
    Geeft de SQL-query terug om een lijst van perioden op te halen die eindigen op een bepaalde datum

    Vereist:
    - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat
    - company_id (int): id van het bedrijf waarin naar periodes gezocht word

    Retourneert:
    - Een lijst met periode ids die eindigen op de vooropgestelde datum
    """
    date = timezone.datetime.fromisoformat(date)
    period_ids = f"""
        SELECT period_id
        FROM periods
        WHERE company_id = {company_id}
        AND (
            end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year}
            OR
            end_date = (
                SELECT MAX(end_date)
                FROM periods AS p2
                WHERE p2.company_id = periods.company_id
                AND DATE_PART('year', p2.end_date) = DATE_PART('year', periods.end_date)
                AND DATE_PART('year', fiscal_year_end) = {date.year}
            )
        );
    """
    return period_ids
    # with get_db_connection() as conn:
    #     with conn.cursor() as cursor:
    #         cursor.execute(period_ids)
    #         period_ids = cursor.fetchall()
    # if len(period_ids) == 0:
    #     return "Dit bedrijf heeft geen periode tijdens deze datum"
    # return period_ids


def account_details(company_id: int = 0, period_id: int = 0, account_id: int = 0):
    """
    Geeft een lijst van accountdetails terug afhankelijk van de company_id, period_id of account_id.
    Opmerking: Niet voor berekeningen zoals EBITDA of eigen vermogen.

    Args:
        company_id (int): De unieke ID van het bedrijf. Vul alleen deze in voor bedrijfsspecifieke details.
        period_id (int): De unieke ID van de periode. Vul alleen deze in voor periodespecifieke details.
        account_id (int): De unieke ID van de account. Vul alleen deze in voor account specifieke details.

    Returns:
        list: Lijst met accountdetails, waaronder company_id, period_id, account_id, account_name, account_number,
              number_without_suffix, original_name, original_number, account_type, reconciliation_template_id,
              value en starred. 'Value' vertegenwoordigt de waarde van het account en is belangrijk bij winst/verliesvragen.
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            if company_id != 0:
                sql = f"""SELECT * FROM account_details WHERE account_details.company_id = {company_id}"""
                cursor.execute(sql)
                return cursor.fetchall()

            elif period_id != 0:
                sql = f"""SELECT * FROM account_details WHERE account_details.period_id = {period_id}"""
                cursor.execute(sql)
                return cursor.fetchall()

            elif account_id != 0:
                sql = f"""SELECT * FROM account_details WHERE account_details.account_id = {account_id}"""
                cursor.execute(sql)
                return cursor.fetchall()


def reconciliation_api_call(company_id: int, date: str):
    """
    Deze tool geeft de reconiliation ids en namen terug van reconiliations van het gegeven bedrijf en datum.

    Vereiste:
        - company_id (int): De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat


    Retourneert:
        - reconiliation ids en namen terug van reconiliations van het gegeven bedrijf en datum.

    """
    date = timezone.datetime.fromisoformat(date)
    period_ids = f"""
            SELECT period_id
            FROM periods
            WHERE company_id = {company_id} and DATE_PART('year', end_date) = {date.year} and DATE_PART('month', end_date) = {date.month}
            AND (
                end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year}
                OR
                end_date = (
                    SELECT MAX(end_date)
                    FROM periods AS p2
                    WHERE p2.company_id = periods.company_id
                    AND DATE_PART('year', p2.end_date) = DATE_PART('year', periods.end_date)
                    AND DATE_PART('year', fiscal_year_end) = {date.year}
                )
            );
        """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(period_ids)
            period_ids = cursor.fetchall()
            period_id = period_ids[0][0]
            print(period_id)
            sql = f"""SELECT reconciliation_id, name
                FROM reconciliations
                WHERE 
                    company_id = {company_id} AND
                    period_id = {period_id};
                """
            cursor.execute(sql)
            records = cursor.fetchall()
            return records


def list_tables():
    """
    Geeft een lijst van alle tabellen in het schema.
    Returns:
        list: Lijst van tabelnamen in het 'public' schema.
    """
    sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    return result


def describe_tables(table_name: str):
    """
    Geeft de kolomnamen en datatypes van de opgegeven tabel.
    Args:
        table_name (str): Naam van de tabel om te beschrijven.
    Returns:
        list: Lijst van kolomnamen en hun datatypes voor de opgegeven tabel.
    """
    sql = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table_name}'"
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            result = cursor.fetchall()
    return result


def load_data(sql_query: str):
    """
    Gebruik deze tool enkel als je geen oplossing kan vinden aan de hand van de andere tools. Voert een aangepaste SQL-query uit en retourneert het resultaat.
    Args:
        sql_query (str): De SQL-query om uit te voeren.
    Returns:
        list: Resultaten van de uitgevoerde query.
    Opmerking:
        Gebruik eerst de functies list_tables en describe_tables voor context.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()
    return result
