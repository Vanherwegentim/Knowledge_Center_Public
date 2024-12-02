from django.utils import timezone

from enums.account_type import AccountType
from utils import (
    get_acount_details_by_account_number,
    get_db_connection,
    get_period_ids,
)


def bereken_EBITDA(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de ebitda te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!
    Vereiste:
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat
        - De company_id van het bedrijf

    Retourneert:
        - De EBITDA

    Details:
    EBITDA, short for earnings before interest, taxes, depreciation, and amortization, is an alternate measure of profitability to net income.
    It's used to assess a company's profitability and financial performance.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            sql = f"""SELECT sum(value)*-1 as EBITDA
                    FROM account_details
                    WHERE 
                        company_id = {company_id} AND
                        account_number SIMILAR TO '60%|61%|62%|64%|70%|71%|72%|73%|74%' AND
                        period_id = {period_id};
                    """
            # cursor.execute(sql)
            # records = cursor.fetchall()
            # gain = sum([float(record[10]) for record in records])
            # result = gain * -1
            return sql



def bereken_VERLIES(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om het verlies te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat
        - De company_id van het bedrijf

    Retourneert:
        - het VERLIES

    Details:
        Wanneer de totale inkomsten lager liggen als de totale uitgaven, dan spreekt men van verlies.
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            sql = f"""SELECT sum(value)*-1 as Verlies
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '60%|61%|62%|63%|64%|65%|66%|67%|68%|70%|71%|72%|73%|74%|75%|76%|77%|78%' AND
                    period_id = {period_id};
                """
            # cursor.execute(sql)
            # records = cursor.fetchall()
            # gain = sum([float(record[10]) for record in records])
            # result = gain * -1
            return sql


def bereken_balanstotaal(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om het balanstotaal te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!
    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het balanstotaal

    Details:
    Balanstotaal is het totaal van alle schulden en bezittingen, passiva en activa van een onderneming
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            sql = f"""SELECT sum(value)*-1 as Balanstotaal
                    FROM account_details
                    WHERE 
                        company_id = {company_id} AND
                        account_type = '{AccountType.ASSET}' AND
                        period_id = {period_id};
                    """
            # cursor.execute(sql)
            # records = cursor.fetchall()
            # result = sum([float(record[0]) for record in records])
            return sql


def bereken_eigen_vermogen(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om het eigen vermogen te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!
    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het eigen vermogen

    Details:
    Het eigen vermogen is het saldo van de bezittingen ('activa') en schulden ('passiva') van een onderneming of organisatie
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            sql = f"""SELECT sum(value)*-1 as Eigen_vermogen
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '10%|11%|12%|13%|14%|15%' AND
                    period_id = {period_id};
                """
            # records = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [10, 11, 12, 13, 14, 15]
            # )
            # result = sum([float(record[0]) for record in records])
            return sql


def bereken_voorzieningen(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de voorzieningen te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - de voorzieningen

    Details:
    De voorzieningen
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            # additives = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [16]
            # )

            # result = sum([float(record[0]) for record in additives])
            sql = f"""SELECT sum(value)*-1 as Voorzieningen
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '16%' AND
                    period_id = {period_id};
                """
            return sql


def bereken_handelswerkkapitaal(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de handelswerkkapitaal te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!
    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het handelswerkkapitaal

    Details:
    Het handelswerkkapitaal omvat de balansposten die nodig zijn voor de bedrijfsvoering, zoals debiteuren en crediteuren (en ook voorraden)
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            # additives = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [30, 31, 32, 33, 34, 35, 36, 37, 40]
            # )
            # negatives = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [44]
            # )

            # result = sum([float(record[0]) for record in additives]) - sum(
            #     [float(record[0]) for record in negatives]
            # )
            sql = f"""SELECT 
                            SUM(CASE WHEN account_number SIMILAR TO '30%|31%|32%|33%|34%|35%|36%|37%|40' 
                                    THEN value 
                                    ELSE 0 
                                END) 
                            - SUM(CASE WHEN account_number LIKE '44%' 
                                    THEN value 
                                    ELSE 0 
                                END) AS handelswerkkapitaal
                        FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    period_id = {period_id};

                        """
            return sql


def bereken_financiele_schulden(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de financiele schulden van een bedrijf te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - de financiele schulden

    Details:
    De financiele schulden zijn een onderverdeling bij de schulden op meer dan één jaar.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            # additives = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [16, 17, 42, 43]
            # )

            # result = sum([float(record[0]) for record in additives])
            sql = f"""
                    SELECT sum(value)*-1 as Financiële_schulden
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '16%|17%|42%|43%' AND
                    period_id = {period_id};
            """
            return sql


def bereken_liquide_middelen(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de liquide middelen van een bedrijf te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!
    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - de financiele schulden

    Details:
    De financiele schulden zijn een onderverdeling bij de schulden op meer dan één jaar.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            # additives = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [50, 51, 52, 53, 54, 55, 56, 57, 58]
            # )

            # result = sum([float(record[0]) for record in additives])
            sql = f"""
                    SELECT sum(value)*-1 as liquide_middelen
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '50%|51%|52%|53%|54%|55%|56%|57%|58%' AND
                    period_id = {period_id};
            """
            return sql


def bereken_bruto_marge(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om het bruto marge van een bedrijf te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het bruto marge

    Details:
    De bruto marge is een verhouding die meet hoe winstgevend uw bedrijf is
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            # additives = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [70, 71, 72, 74]
            # )
            # negatives = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [60]
            # )

            # result = sum([float(record[0]) for record in additives]) - sum(
            #     [float(record[0]) for record in negatives]
            # )
            sql = f"""SELECT 
                            SUM(CASE WHEN account_number SIMILAR TO '70%|71%|72%|74%' 
                                    THEN value 
                                    ELSE 0 
                                END) 
                            - SUM(CASE WHEN account_number LIKE '60%' 
                                    THEN value 
                                    ELSE 0 
                                END) AS Bruto_marge
                        FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    period_id = {period_id};

                        """
            return sql


def bereken_omzet(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de omzet te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat
        - De company_id van het bedrijf

    Retourneert:
        - De OMZET

    Details:
    De omzet van uw bedrijf is het totale bedrag aan inkomsten uit de verkoop van producten en diensten in een bepaalde periode. Dit wordt ook wel de bruto-omzet genoemd.
    """

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            sql = f"""SELECT sum(value)*-1 as Omzet
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '70%' AND
                    period_id = {period_id};
                """
            # cursor.execute(sql)
            # records = cursor.fetchall()
            # gain = sum([float(record[10]) for record in records])
            # result = gain * -1
            return sql


def bereken_EBITDA_marge(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de EBITDA marge te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - De EBITDA marge

    Details:
    De EBITDA marge geeft aan hoeveel cash een bedrijf genereert voor elke euro omzet.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            sql = f"""WITH 
                    ebitda AS (
                        SELECT 
                            company_id,
                            SUM(value) * -1 AS ebitda_value
                        FROM account_details
                        WHERE 
                            company_id = {company_id} AND
                            account_number SIMILAR TO '60%|61%|62%|64%|70%|71%|72%|73%|74%' AND
                            period_id = {period_id}
                        GROUP BY company_id
                    ),
                    marge AS (
                        SELECT 
                            company_id,
                            SUM(value) * -1 AS marge_value
                        FROM account_details
                        WHERE 
                            company_id = {company_id} AND
                            account_number SIMILAR TO '70%' AND
                            period_id = {period_id}
                        GROUP BY company_id
                    )
                SELECT 
                    e.company_id,  
                    m.marge_value, 
                    CASE 
                        WHEN m.marge_value <> 0 THEN e.ebitda_value / m.marge_value
                        ELSE NULL 
                    END AS ebitda_marge
                FROM 
                    ebitda e
                JOIN 
                    marge m 
                ON 
                    e.company_id = m.company_id;

                            
                            """
    return sql
    # ebitda = bereken_EBITDA(company_id, date)
    # omzet = bereken_omzet(company_id, date)
    # return ebitda / omzet


def bereken_afschrijvingen(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de afschrijvingen te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - De afschrijvingen

    Details:
    De afschrijvingen
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            # additives = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [63]
            # )

            # result = sum([float(record[0]) for record in additives])
            sql = f"""SELECT sum(value)*-1 as Afschrijving
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '63%' AND
                    period_id = {period_id};
                """
            return sql


def bereken_EBIT(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de EBIT te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - De EBITDA marge

    Details:
    De EBITDA marge geeft aan hoeveel cash een bedrijf genereert voor elke euro omzet.
    """
    # ebitda = bereken_EBITDA(company_id, date)
    # afschrijvingen = bereken_afschrijvingen(company_id, date)
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
    sql = f"""SELECT sum(value)*-1 as EBIT
                    FROM account_details
                    WHERE 
                        company_id = {company_id} AND
                        account_number SIMILAR TO '60%|61%|62%|63%|64%|70%|71%|72%|73%|74%' AND
                        period_id = {period_id};
                    """
    # return ebitda + afschrijvingen
    return sql


def bereken_netto_financiele_schuld(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de netto financiele schuld te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - De netto financiele schuld

    Details:
    De netto financiele schuld geeft het vermogen van de groep weer om de schulden terug te betalen op basis van de kasstromen gegenereerd door de bedrijfsactiviteiten
    """
    # schulden = bereken_financiele_schulden(company_id, date)
    # liquide = bereken_liquide_middelen(company_id, date)
    # return schulden - liquide
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
    sql = f"""
                WITH financiele_schuld as (SELECT company_id, sum(value)*-1 as schuld_value
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '16%|17%|42%|43%' AND
                    period_id = {period_id}
                GROUP BY company_id
),
            liquide_middelen as (SELECT company_id, sum(value)*-1 as liquid_value
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '50%|51%|52%|53%|54%|55%|56%|57%|58%' AND
                    period_id = {period_id}
                GROUP BY company_id
)
                SELECT schuld_value - liquid_value as financiele_schuld
                FROM financiele_schuld as f join liquide_middelen as l on f.company_id = l.company_id
                    ;
            """
    return sql


def bereken_handelsvorderingen(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de handelvorderingen van een bedrijf te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!
    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het handelsvorderingen

    Details:
    Het handelsvorderingen zijn een boekhoudkundige rekening met alle uitstaande geldclaims die betrekking hebben op verkopen waarvan de betaling nog niet geïnd is
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            # additives = get_acount_details_by_account_number(
            #     cursor, company_id, period_id, [40]
            # )

            # result = sum([float(record[0]) for record in additives])
            # return result
            sql = f"""SELECT sum(value)*-1 as handelsvordering
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '40%' AND
                    period_id = {period_id};
                """
            return sql


def bereken_dso(company_id: int, date: str):
    """
    Deze tool geeft de SQL-query terug om de Day Sales Outstanding (DSO) te berekenen. GEBRUIK DE LOAD_DATA TOOL OM DE TERUGGEGEVEN SQL UIT TE VOEREN!!!!

    Vereiste:
        - De company_id van het bedrijf
        - date (str): eind datum van de gezochte periode in YYYY-MM-DD formaat

    Retourneert:
        - Het handelsvorderingen

    Details:
    De DSO geeft aan hoeveel dagen het gemiddeld duurt voordat een factuur betaald is nadat jouw bedrijf een product of dienst heeft geleverd
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            period_id = get_period_ids(cursor, company_id, date)
            if isinstance(period_id, str):  # If the result is the error message
                return period_id
            
    sql = f"""WITH 
    value_40 AS (
        SELECT c.company_id, c.name, COALESCE(SUM(ad.value), 0) AS total_value_40
        FROM companies c
        JOIN account_details ad ON c.company_id = ad.company_id
        WHERE ad.account_number SIMILAR TO '40%' 
          AND ad.company_id = {company_id} 
          AND ad.period_id = {period_id}
        GROUP BY c.company_id, c.name
    ),
    value_70 AS (
        SELECT c.company_id, COALESCE(SUM(ad.value), 0) AS total_value_70
        FROM companies c
        JOIN account_details ad ON c.company_id = ad.company_id
        WHERE ad.account_number SIMILAR TO '70%' 
          AND ad.company_id = {company_id} 
          AND ad.period_id = {period_id}
        GROUP BY c.company_id
    )
SELECT 
    v40.name, 
    CASE 
        WHEN v70.total_value_70 <> 0 THEN abs(v40.total_value_40 / v70.total_value_70) * 365
        ELSE NULL 
    END AS result_ratio
FROM value_40 v40
JOIN value_70 v70 ON v40.company_id = v70.company_id;

                """
    return sql
