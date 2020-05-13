import dash
import dash_html_components as html

from xero_dash.dashboard import (
    analysis_customer_churn,
    analysis_geographic_location,
    analysis_plan_upgrade_and_downgrade,
    intro_words,
)

external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(
    [
        *intro_words(),
        *analysis_customer_churn(),
        *analysis_plan_upgrade_and_downgrade(),
        *analysis_geographic_location(),
    ]
)
