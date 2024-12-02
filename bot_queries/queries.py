from datetime import datetime

# def group_ebitda(date_str: str, limit: int = 10, order_by: str = "DESC") -> str:
#     """
#     Returns an SQL query to compare companies based on EBITDA for a specified fiscal year.

#     The query calculates EBITDA by summing the values of specific account numbers for each company.
#     It then orders the companies by their total EBITDA in ascending or descending order.

#     Use the `load_data` method with the returned query to fetch data from the database.

#     Args:
#         date_str (str): End date of the period in "YYYY-MM-DD" format.
#         limit (int, optional): Number of companies to retrieve. Must be a positive integer. Defaults to 10.
#         order_by (str, optional): Sort order, either "ASC" or "DESC". Defaults to "DESC".

#     Returns:
#         str: The SQL query string.

#     Raises:
#         ValueError: If input parameters are invalid.
#     """
#     # Validate date
#     try:
#         date = datetime.fromisoformat(date_str)
#     except ValueError:
#         raise ValueError("Invalid date format. Please use 'YYYY-MM-DD'.")

#     # Validate limit
#     if not isinstance(limit, int) or limit <= 0:
#         raise ValueError("Limit must be a positive integer.")

#     # Validate order_by
#     order_by = order_by.upper()
#     if order_by not in ("ASC", "DESC"):
#         raise ValueError("order_by must be 'ASC' or 'DESC'.")

#     query = f"""
#     WITH latest_period AS (
#         SELECT
#             period_id,
#             company_id,
#             ROW_NUMBER() OVER (
#                 PARTITION BY company_id 
#                 ORDER BY 
#                     CASE
#                         WHEN end_date = fiscal_year_end AND EXTRACT(YEAR FROM fiscal_year_end) = {date.year} THEN 1
#                         ELSE 2
#                     END,
#                     end_date DESC
#             ) AS rn
#         FROM periods
#         WHERE EXTRACT(YEAR FROM end_date) = {date.year}
#     )
#     SELECT
#         c.company_id,
#         c.name,
#         SUM(ad.value) AS total_ebitda
#     FROM companies c
#     JOIN account_details ad ON c.company_id = ad.company_id
#     JOIN periods p ON ad.period_id = p.period_id
#     JOIN latest_period lp ON lp.company_id = c.company_id AND ad.period_id = lp.period_id AND lp.rn = 1
#     WHERE ad.account_number SIMILAR TO '60%|61%|62%|64%|70%|71%|72%|73%|74%'
#     GROUP BY c.company_id, c.name
#     ORDER BY total_ebitda {order_by}
#     LIMIT {limit};
#     """
#     return query


def voorafbetaling(term:int, year:int) -> str:
    """
    Returns an SQL query to retrieve the prepayments (voorafbetalingen) for a specific term (quarter) for all dossiers/companies.
    Note: The terms "dossier" and "bedrijf" are interchangeable and refer to the same concept.

    For each term, there is a different column named prep1_made, prep2_made, prep3_made, or prep4_made.
    Adjust this query as needed to obtain the desired results.

    Use the load_data method with the returned query to fetch data from the database.

    Arguments:
        term (int): The specific term, can be 0, 1, 2, 3, or 4. If the term is 0, all terms are returned.
        year (int): The year for which the data is required, must be between 2000 and 2099.

    Returns:
        str: The SQL query as a string.

    Errors:
        ValueError: If the input parameters are invalid.
    """
    # Validate date
    
    if not (2000 <= year <= 2099):
        raise ValueError("Year must be in the form 20.. (e.g., 2023)")
    # Validate limit
    if not(term == 0 or term == 1 or term == 2 or term == 3 or term == 4):
        raise ValueError("Term must be 1,2,3 or 4")
    else:
        if term == 0:
            quart = "prep1_made, prep2_made, prep3_made, prep4_made"
        if term == 1:
            quart = "prep1_made"
        if term == 2:
            quart = "prep2_made"
        if term == 3:
            quart = "prep3_made"
        if term == 4:
            quart = "prep4_made"

    query = f"""
            WITH LatestEntries AS (
    SELECT 
        c.name,
        p.end_date, 
        {quart},
        ROW_NUMBER() OVER (PARTITION BY r.company_id ORDER BY p.end_date DESC) AS rn
    FROM 
        public.reconciliation_results AS r
    JOIN 
        public.periods AS p  
    ON 
        r.period_id = p.period_id
    JOIN 
        public.companies AS c 
    ON 
        r.company_id = c.company_id
    WHERE 
        EXTRACT(YEAR FROM p.end_date) = {year}
)
SELECT 
    name,
    end_date, 
    {quart}
FROM 
    LatestEntries
WHERE 
    rn = 1;

    """
    return query
