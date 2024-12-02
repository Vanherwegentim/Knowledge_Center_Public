import psycopg2
import psycopg2._psycopg
import streamlit as st
from django.utils import timezone
from psycopg2._psycopg import cursor


def get_db_connection() -> psycopg2._psycopg.connection:
    return psycopg2.connect(
        database=st.secrets["RDS_NAME"],
        host=st.secrets["RDS_HOST"],
        password=st.secrets["RDS_PWD"],
        port=st.secrets["RDS_PORT"],
        user=st.secrets["RDS_USER"],
    )


def get_period_ids(cursor: cursor, company_id: int, date: str):
    try:
        date = timezone.datetime.fromisoformat(date)
        period_ids_query = f"""
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
        cursor.execute(period_ids_query)
        period_ids = cursor.fetchall()

        # Logical check if no period is found
        if len(period_ids) == 0:
            return "Dit bedrijf heeft geen periode tijdens deze datum"

        return period_ids[0][0]

    except Exception as e:
        # Handle exceptions like database connectivity issues, SQL errors, etc.
        return f"Er is een fout opgetreden: {str(e)}"


def get_acount_details_by_account_number(
    cursor: cursor, company_id: int, period_id: int, number_filter: list[int]
):

    number_filter_str = "%|".join([str(i) for i in number_filter])
    number_filter_str += "%"
    print(number_filter_str)
    sql = f"""SELECT value
                FROM account_details
                WHERE 
                    company_id = {company_id} AND
                    account_number SIMILAR TO '{number_filter_str}' AND
                    period_id = {period_id};
                """
    cursor.execute(sql)
    records = cursor.fetchall()
    return records
