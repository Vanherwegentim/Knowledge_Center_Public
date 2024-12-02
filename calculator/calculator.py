from .calculations import (
    bereken_afschrijvingen,
    bereken_balanstotaal,
    bereken_bruto_marge,
    bereken_dso,
    bereken_EBIT,
    bereken_EBITDA,
    bereken_EBITDA_marge,
    bereken_eigen_vermogen,
    bereken_financiele_schulden,
    bereken_handelsvorderingen,
    bereken_handelswerkkapitaal,
    bereken_liquide_middelen,
    bereken_netto_financiele_schuld,
    bereken_omzet,
    bereken_VERLIES,
    bereken_voorzieningen,
)
from utils import get_db_connection
from django.utils import timezone
import streamlit as st
import pandas as pd
calculations = {
    "EBITDA": bereken_EBITDA,
    "verlies": bereken_VERLIES,
    "balanstotaal": bereken_balanstotaal,
    "eigen vermogen": bereken_eigen_vermogen,
    "voorzieningen": bereken_voorzieningen,
    "handelswerkkapitaal": bereken_handelswerkkapitaal,
    "financiele schulden": bereken_financiele_schulden,
    "liquide middelen": bereken_liquide_middelen,
    "bruto marge": bereken_bruto_marge,
    "omzet": bereken_omzet,
    "EBITDA marge": bereken_EBITDA_marge,
    "afschrijvingen": bereken_afschrijvingen,
    "EBIT": bereken_EBIT,
    "netto financiele schuld": bereken_netto_financiele_schuld,
    "handelsvorderingen": bereken_handelsvorderingen,
    "dso": bereken_dso,
}

def load_data(sql_query: str):
    """
    Voert een SQL-query uit en retourneert het resultaat. Het resultaat kan groter zijn dan jouw context window dus krijg jij een preview van de data terwijl de volledige data naast jouw antwoord wordt getoond in een pandas dataframe. Je hoeft je resultaat niet te tonen, enkel herkennen dat de functie succesvol. Jij krijgt een preview zodat als er vragen zijn jij die veranderen kan doorvoeren.
    Args:
        sql_query (str): De SQL-query om uit te voeren.
    Returns:
        
    Opmerking:
        Gebruik eerst de functies list_tables en describe_tables voor context.
    """
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchall()
            full_df = pd.DataFrame(result)
            st.session_state.data = full_df
            st.session_state.data.columns =[ x[0] for x in cursor.description ]
            return "Het volgende is een preview van data, de user krijgt de hele data te zien. Jij, de chatbot krijgt een deel omdat er anders het risico is om jou context window te overflowen. Vermeld in je antwoord dat jij een preview hebt van de data en de volledige data rechts van de chat te vinden is!" +  str(full_df.head(1))

def bereken(what: str, company_id: int, date: str):
    """
    Voert een specifieke berekening uit voor een bedrijf in een bepaalde periode.
    Args:
        what (str): Het type berekening (EBITDA, verlies, balanstotaal, eigen vermogen, voorzieningen, handelswerkkapitaal, financiele schulden, liquide middelen, bruto marge, omzet, EBITDA marge, afschrijvingen, netto financiele schuld, handelsvorderingen, dso)
        company_id (int): De ID van het bedrijf. Gebruik de period_tool om de ID te verkrijgen als je alleen de bedrijfsnaam hebt.
        date (str): Einddatum van de periode in "YYYY-MM-DD" formaat.
    Returns:
        float: Het resultaat van de berekening.
    Foutafhandeling:
        Geeft een foutbericht als het gevraagde type berekening niet wordt ondersteund.
    """

    # TODO: Vergelijk mogelijke synoniemen/typefouten met sleutelwoorden in calculations (gebruik cosine similarity of LLM).

    if what in calculations:
        return calculations[what](company_id, date)

    return f"Kan de berekening voor '{what}' niet uitvoeren. Alleen de volgende berekeningen worden ondersteund: {list(calculations.keys())}"


def vergelijk_op_basis_van(
    what: str, date: str, limit: int = 10, order_by: str = "DESC"
):
    """
    Geeft de gevraagde hoeveelheid bedrijven terug gesorteerd op ASC or DESC voor een bepaalde periode
    Vereiste:
        - what (str): Het soort berekening dat gemaakt moet worden. Map indien mogelijk naar een van volgende woorden (EBITDA, verlies, balanstotaal, eigen vermogen, voorzieningen,
            handelswerkkapitaal, financiele schulden, liquide middelen, bruto marge, omzet, EBITDA marge, afschrijvingen, netto financiele schuld, handelsvorderingen, dso)
    """
    date = timezone.datetime.fromisoformat(date)
    if limit > 100:
        return (
            "Dit is een te groot aantal bedrijven. Kies aub een kleinere hoeveelheid."
        )
    match what:
        case "EBITDA":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                            ROW_NUMBER() OVER (
                                                PARTITION BY company_id 
                                                ORDER BY 
                                                    (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                                    end_date DESC
                                            ) AS rn
                                        FROM periods
                                        WHERE DATE_PART('year', end_date) = {date.year}
                                    )

                                    SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                                    FROM companies c
                                    JOIN account_details ad ON c.company_id = ad.company_id
                                    JOIN periods p ON ad.period_id = p.period_id
                                    JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                                    WHERE ad.account_number SIMILAR TO '60%|61%|62%|64%|70%|71%|72%|73%|74%'
                                    GROUP BY c.company_id, c.name
                                    ORDER BY total_value {order_by}
                                    LIMIT {limit};
                                    """
        case "verlies":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '60%|61%|62%|63%|64%|65%|66%|67%|68%|70%|71%|72%|73%|74%|75%|76%|77%|78%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "balanstotaal":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '60%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "eigen vermogen":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '10%|11%|12%|13%|14%|15%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "voorziening":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '16%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "handelswerkkapitaal":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT 
                            c.company_id, 
                            c.name, 
                            SUM(CASE WHEN ad.account_number SIMILAR TO '30%|31%|32%|33%|34%|35%|36%|37%|40' 
                                    THEN ad.value 
                                    ELSE 0 
                                END) 
                            - SUM(CASE WHEN ad.account_number LIKE '44%' 
                                    THEN ad.value 
                                    ELSE 0 
                                END) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "financiele schulden":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '16%|17%|42%|43%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "liquide middelen":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '50%|51%|52%|53%|54%|55%|56%|57%|58%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "bruto marge":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT 
                            c.company_id, 
                            c.name, 
                            SUM(CASE WHEN ad.account_number SIMILAR TO '70%|71%|72%|74%' 
                                    THEN ad.value 
                                    ELSE 0 
                                END) 
                            - SUM(CASE WHEN ad.account_number LIKE '60%' 
                                    THEN ad.value 
                                    ELSE 0 
                                END) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "omzet":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '70%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "EBITDA marge":
            sql = f"""WITH latest_period AS (
                    SELECT period_id, company_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY company_id 
                            ORDER BY 
                                (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                end_date DESC
                        ) AS rn
                    FROM periods
                    WHERE DATE_PART('year', end_date) = {date.year}
                ),

                -- Bereken de EBITDA voor elk bedrijf
                ebitda AS (
                    SELECT c.company_id, c.name, SUM(ad.value) AS ebitda_value
                    FROM companies c
                    JOIN account_details ad ON c.company_id = ad.company_id
                    JOIN periods p ON ad.period_id = p.period_id
                    JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                    WHERE ad.account_number SIMILAR TO '60%|61%|62%|64%|70%|71%|72%|73%|74%'
                    GROUP BY c.company_id, c.name
                ),

                -- Bereken de omzet voor elk bedrijf
                marge AS (
                    SELECT c.company_id, SUM(ad.value) AS marge_value
                    FROM companies c
                    JOIN account_details ad ON c.company_id = ad.company_id
                    JOIN periods p ON ad.period_id = p.period_id
                    JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                    WHERE ad.account_number SIMILAR TO '70%'
                    GROUP BY c.company_id
                )

                -- Combineer EBITDA en marge om de EBITDA-marge te berekenen
                SELECT e.company_id, 
                    e.name, 
                    e.ebitda_value, 
                    m.marge_value, 
                    CASE 
                        WHEN m.marge_value <> 0 THEN e.ebitda_value / m.marge_value
                        ELSE NULL 
                    END AS ebitda_marge
                FROM ebitda e
                JOIN marge m ON e.company_id = m.company_id
                ORDER BY ebitda_marge {order_by}
                LIMIT {limit};
                """
        case "afschrijvingen":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        )

                        SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                        FROM companies c
                        JOIN account_details ad ON c.company_id = ad.company_id
                        JOIN periods p ON ad.period_id = p.period_id
                        JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                        WHERE ad.account_number SIMILAR TO '63%'
                        GROUP BY c.company_id, c.name
                        ORDER BY total_value {order_by}
                        LIMIT {limit};
                        """
        case "EBIT":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        ),

                        -- CTE to calculate the total_value for account numbers starting with '60%|61%|62%|64%|70%|71%|72%|73%|74%'
                        value_main AS (
                            SELECT c.company_id, c.name, SUM(ad.value) AS total_value_main
                            FROM companies c
                            JOIN account_details ad ON c.company_id = ad.company_id
                            JOIN periods p ON ad.period_id = p.period_id
                            JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                            WHERE ad.account_number SIMILAR TO '60%|61%|62%|64%|70%|71%|72%|73%|74%'
                            GROUP BY c.company_id, c.name
                        ),

                        -- CTE to calculate the total_value for account numbers starting with '63%'
                        value_63 AS (
                            SELECT c.company_id, SUM(ad.value) AS total_value_63
                            FROM companies c
                            JOIN account_details ad ON c.company_id = ad.company_id
                            JOIN periods p ON ad.period_id = p.period_id
                            JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                            WHERE ad.account_number SIMILAR TO '63%'
                            GROUP BY c.company_id
                        )

                        -- Main query to join the results and perform the subtraction
                        SELECT v_main.company_id, 
                            v_main.name, 
                            v_main.total_value_main, 
                            v_63.total_value_63,
                            v_main.total_value_main - v_63.total_value_63 AS result_difference
                        FROM value_main v_main
                        JOIN value_63 v_63 ON v_main.company_id = v_63.company_id
                        ORDER BY result_difference {order_by}
                        LIMIT {limit};
                        """
        case "Netto financiele schuld":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                ROW_NUMBER() OVER (
                                    PARTITION BY company_id 
                                    ORDER BY 
                                        (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                        end_date DESC
                                ) AS rn
                            FROM periods
                            WHERE DATE_PART('year', end_date) = {date.year}
                        ),

                        -- CTE to calculate the total_value for account numbers starting with '16%|17%|42%|43%'
                        value_1 AS (
                            SELECT c.company_id, c.name, SUM(ad.value) AS total_value_1
                            FROM companies c
                            JOIN account_details ad ON c.company_id = ad.company_id
                            JOIN periods p ON ad.period_id = p.period_id
                            JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                            WHERE ad.account_number SIMILAR TO '16%|17%|42%|43%'
                            GROUP BY c.company_id, c.name
                        ),

                        -- CTE to calculate the total_value for account numbers starting with '50%|51%|52%|53%|54%|55%|56%|57%|58%'
                        value_2 AS (
                            SELECT c.company_id, SUM(ad.value) AS total_value_2
                            FROM companies c
                            JOIN account_details ad ON c.company_id = ad.company_id
                            JOIN periods p ON ad.period_id = p.period_id
                            JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                            WHERE ad.account_number SIMILAR TO '50%|51%|52%|53%|54%|55%|56%|57%|58%'
                            GROUP BY c.company_id
                        )

                        -- Main query to join the results and perform the subtraction
                        SELECT v1.company_id, 
                            v1.name, 
                            v1.total_value_1, 
                            v2.total_value_2,
                            v1.total_value_1 - v2.total_value_2 AS result_difference
                        FROM value_1 v1
                        JOIN value_2 v2 ON v1.company_id = v2.company_id
                        ORDER BY result_difference {order_by}
                        LIMIT {limit};
                        """
        case "handelsvorderingen":
            sql = f"""WITH latest_period AS (
                            SELECT period_id, company_id,
                                            ROW_NUMBER() OVER (
                                                PARTITION BY company_id 
                                                ORDER BY 
                                                    (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                                    end_date DESC
                                            ) AS rn
                                        FROM periods
                                        WHERE DATE_PART('year', end_date) = {date.year}
                                    )

                                    SELECT c.company_id, c.name, SUM(ad.value) AS total_value
                                    FROM companies c
                                    JOIN account_details ad ON c.company_id = ad.company_id
                                    JOIN periods p ON ad.period_id = p.period_id
                                    JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                                    WHERE ad.account_number SIMILAR TO '40%'
                                    GROUP BY c.company_id, c.name
                                    ORDER BY total_value {order_by}
                                    LIMIT {limit};
                                    """
        case "dso":
            sql = f"""WITH latest_period AS (
                    SELECT period_id, company_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY company_id 
                            ORDER BY 
                                (CASE WHEN end_date = fiscal_year_end AND DATE_PART('year', fiscal_year_end) = {date.year} THEN 1 ELSE 2 END),
                                end_date DESC
                        ) AS rn
                    FROM periods
                    WHERE DATE_PART('year', end_date) = {date.year}
                ),

                -- First CTE to calculate the total_value for account numbers starting with '40%'
                value_40 AS (
                    SELECT c.company_id, c.name, SUM(ad.value) AS total_value_40
                    FROM companies c
                    JOIN account_details ad ON c.company_id = ad.company_id
                    JOIN periods p ON ad.period_id = p.period_id
                    JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                    WHERE ad.account_number SIMILAR TO '40%'
                    GROUP BY c.company_id, c.name
                ),

                -- Second CTE to calculate the total_value for account numbers starting with '70%'
                value_70 AS (
                    SELECT c.company_id, SUM(ad.value) AS total_value_70
                    FROM companies c
                    JOIN account_details ad ON c.company_id = ad.company_id
                    JOIN periods p ON ad.period_id = p.period_id
                    JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
                    WHERE ad.account_number SIMILAR TO '70%'
                    GROUP BY c.company_id
                )

                -- Main query to join the results and perform the division
                SELECT v40.name, 
                    CASE 
                        WHEN v70.total_value_70 <> 0 THEN abs(v40.total_value_40 / v70.total_value_70) * 365
                        ELSE NULL 
                    END AS result_ratio
                FROM value_40 v40
                JOIN value_70 v70 ON v40.company_id = v70.company_id
                WHERE v70.total_value_70 <> 0
                ORDER BY result_ratio {order_by}
                LIMIT {limit};
                """

    return load_data(sql)



