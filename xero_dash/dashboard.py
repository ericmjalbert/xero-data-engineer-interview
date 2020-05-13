import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go

from sqlalchemy import text
from libraries.database import session_scope


def intro_words():

    title = html.H1("Xero Technical Project")
    intro = dcc.Markdown(
        """
        This is the analysis of the Log data. I've written an ETL though
        Airflow that can be viewed [here][1]. This ETL will show the
        individual steps that have been done to cleanup and transform the log
        data into Bronze, Silver, and Gold tables.

        [1]: http://localhost:8080/admin/airflow/graph?dag_id=log_data_processing&execution_date=
        """
    )
    return [html.Hr(), title, intro]


def analysis_customer_churn():

    problem = dcc.Markdown(
        """
        ## Customer Churn
        ### Monthly Churn Count

        A customer is deemed to have stopped using Hubdoc if they have not logged
          into the system within thirty days.

        * Provide the total number of customers who have stopped using Hubdoc month
          by month. If a customer’s last login was February 10th, 2019 and we see a
          thirty day period where no additional logins took place then we would say
          that customer has stopped using Hubdoc in February. For the purpose of
          this exercise once a customer has been inactive for 30 days they will not
          appear in the logs again. Assume that you are doing this reporting as of
          January 1st 2020.
        """
    )

    with session_scope() as transaction:
        query = text(
            """
            SELECT
                DATE_TRUNC('month', churn_date AT TIME ZONE 'EST') AS month,
                COUNT(DISTINCT account) AS churned_accounts
            FROM churn_event
            GROUP BY 1
            ORDER BY 1
            """
        )
        results = transaction.execute(query).fetchall()
        x = [row[0] for row in results]
        y = [row[1] for row in results]

    writeup = dcc.Markdown(
        f"""
        #### Assumptions:

        * A customer has to login to do any kind of activity on Hubdoc. This
            means that we can use just log_type=login to track active and
            churned customer
        * An active user is any user that is NOT churned
        * For the purpose of this exercise once a customer has been inactive
            for 30 days they will not appear in the logs again.
        * Every Calculation is done based in UTC time, but presented in EST time
        * Ignore churn for logins in the most recent 30 days of data (Dec 01 2019)

        #### Solution:
        ```sql
        {query.text}
        ```
        """
    )

    graph = dcc.Graph(
        id="churn_graph",
        figure={
            "data": [{"x": x, "y": y, "type": "line", "name": "Churned"}],
            "layout": {"title": "Monthly Churned Accounts"},
        },
    )

    problem2 = dcc.Markdown(
        """
        ### Monthly Churn Rate

        Customer churn rate is defined as the number of churned customers over a
          given period divided by the number of active customers at the end of a
          given period.
        * Provide monthly customer churn rate from January 1, 2019 to
            December 31, 2019. Assume that you are doing this reporting as
            of January 1st 2020.
            * Example: January:0.03, February: 0.05 ...
        """
    )

    with session_scope() as transaction:
        query = text(
            """
            WITH active_accounts AS (
                SELECT
                    DATE_TRUNC('month', date AT TIME ZONE 'EST') AS month,
                    COUNT(DISTINCT account) AS accounts
                FROM login
                GROUP BY 1
            ),

            churned_accounts AS (
                SELECT
                    DATE_TRUNC('month', churn_date AT TIME ZONE 'EST') AS month,
                    COUNT(DISTINCT account) AS churned_accounts
                FROM churn_event
                GROUP BY 1
            )

            SELECT
                month,
                1.0 * COALESCE(churned_accounts, 0) / COALESCE(accounts, 0) AS churn_rate
            FROM active_accounts AS aa
            FULL JOIN churned_accounts AS ca
                USING (month)
            ORDER BY 1
            """
        )
        results = transaction.execute(query).fetchall()
        x = [row[0] for row in results]
        y = [row[1] for row in results]

    writeup2 = dcc.Markdown(
        f"""
        #### Assumptions:

        Same assumptions as before.

        #### Solution:
        ```sql
        {query.text}
        ```
        """
    )

    graph2 = dcc.Graph(
        id="churn-graph2",
        figure={
            "data": [{"x": x, "y": y, "type": "line"}],
            "layout": {"title": "Monthly Churn Rate"},
        },
    )

    return [
        html.Hr(),
        html.Hr(),
        problem,
        writeup,
        graph,
        html.Hr(),
        problem2,
        writeup2,
        graph2,
    ]


def analysis_plan_upgrade_and_downgrade():

    problem = dcc.Markdown(
        """
        ## Plan Upgrade and Downgrade Analysis

        #### Plan Distribution At Signup

        It is important to understand what plans our customers are signing up for
          initially.
        * Provide the total number of customers who selected a plan when they signed
          up and what percentage of total signups that represents. Calculate this
          for all of 2019.
          * Example: assuming we have 2 plans: X. and Y
              Plan X: 400 signups, represents 40% of total signups.
              Plan Y: 600 signups, represents 60% of total signups.
        """
    )

    query = text(
        """
        SELECT
            plan,
            COUNT(DISTINCT account) AS signups
        FROM signup
        GROUP BY 1
        """
    )

    writeup = dcc.Markdown(
        f"""
        #### Assumptions:

        * No new assumptions

        #### Solution:
        ```sql
        {query.text}
        ```
        """
    )

    with session_scope() as transaction:
        results = transaction.execute(query).fetchall()
        labels = [row[0] for row in results]
        values = [row[1] for row in results]

    fig = go.Figure(data=[go.Pie(labels=labels, values=values)])
    fig.update_layout(title_text="Plan Selections At Signup", title_x=0.5)
    graph = dcc.Graph(id="plan-graph", figure=fig)

    problem2 = dcc.Markdown(
        """
        #### Plan Changes and Average Change

        We also want to understand how customers change plans after initial sign up.
        * Provide a breakdown of how many customers changed from their initial
          signup plan to another plan and the average number of days that customers
          took to make that change.
          * Example: assuming we have 2 plans: X. and Y
              Plan Y to Plan X: 80 customers. Average time to change: 50 days.
              Plan X to Plan Y: 0 customers. Average time to change: N/A.
        """
    )

    query = text(
        """
        SELECT
            pc.details_from || ' to ' || pc.details_to AS plan_change,
            COUNT(DISTINCT pc.account) AS customers,
            AVG(DATE_PART('day',
                    (pc.details_change_date - signup.date)
                )) AS avg_days_to_change
        FROM plan_change AS pc
        INNER JOIN signup
            USING (account)
        GROUP BY 1
        ORDER BY 1
        """
    )

    writeup2 = dcc.Markdown(
        f"""
        #### Assumptions:

        * Assuming if a user changes, it will only happen once. This is true
            in the current dataset, but might not hold true future data.

        #### Solution:
        ```sql
        {query.text}
        ```
        """
    )

    with session_scope() as transaction:
        results = transaction.execute(query).fetchall()

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=["Plan Changes", "Customers", "Avg. Days to Change"]
                ),
                cells=dict(
                    values=[
                        [row[0] for row in results],
                        [row[1] for row in results],
                        [row[2] for row in results],
                    ]
                ),
            )
        ]
    )
    fig.update_layout(title_text="Plan Changes", title_x=0.5)
    graph2 = dcc.Graph(id="plan-graph2", figure=fig)

    return [
        html.Hr(),
        html.Hr(),
        problem,
        writeup,
        graph,
        html.Hr(),
        problem2,
        writeup2,
        graph2,
    ]


def analysis_geographic_location():
    problem = dcc.Markdown(
        """
        ## Geographic Location Analysis

        It is important for our sales and marketing teams to know the geological
          demographic of our customer base as it provides insights on how to
          advertise and provide support for the product.
        * Provide the number of active customers in each province at the end of the
          year.
              * Example: AB:84, PE:8, ON:224, ...
        """
    )

    query = text(
        """
        SELECT province, signups, active_users
        FROM province_by_year
        WHERE year = '2019-01-01'
        ORDER BY 1
        """
    )

    writeup = dcc.Markdown(
        f"""
        #### Assumptions:

        *All addresses follow the format of:
            ```
            street, city, province <whitespace> postal_code
            ```
        *All postal_code's are canadian postal codes:
            ```
            <3 alphanumeric chars> <whitespace> <3 alphanumeric chars>
            ```
        *An active user is any user that is NOT churned

        #### Solution:
        ```sql
        {query.text}
        ```
        """
    )

    with session_scope() as transaction:
        results = transaction.execute(query).fetchall()
        provinces = [row[0] for row in results]
        signups = [row[1] for row in results]
        actives = [row[2] for row in results]

    graph = dcc.Graph(
        id="geo_graph",
        figure={
            "data": [
                {"x": provinces, "y": signups, "type": "bar", "name": "Signups"},
                {"x": provinces, "y": actives, "type": "bar", "name": "Active Users"},
            ],
            "layout": {"title": "2019 Accounts By Province"},
        },
    )

    return [html.Hr(), problem, writeup, graph, html.Hr()]
