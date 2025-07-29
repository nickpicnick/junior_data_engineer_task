from typing import List, Optional, Dict, Any
from dash import Dash, dash_table, dcc, html, Input, Output
from sqlalchemy import create_engine, text
from dash.development.base_component import Component

# init dash app
app = Dash(__name__)

# Connect to PostgreSQL
DB_URL = "postgresql://admin:admin@localhost:5432/countries_db"
engine = create_engine(DB_URL)

# read data
def load_data() -> List[Dict]:
    """
    Load country summary data from the database view.

    Returns:
        List of dictionaries.
    """
    with engine.connect() as con:
        query_result = con.execute(text("SELECT * FROM countries_summary;"))
        return [dict(row) for row in query_result.mappings()]

# get regions for additional filtering
def get_regions() -> List[str]:
    """Get unique regions from the DB"""
    with engine.connect() as con:
        query_result = con.execute(text("SELECT DISTINCT \"Region\" FROM countries_summary ORDER BY \"Region\";"))
        return [row[0] for row in query_result] 

display_data = load_data()
columns = [{"name": key, "id": key} for key in display_data[0].keys() if key != "flag_url"]
regions = get_regions()

# layout
app.layout = html.Div([
    html.H1("Countries Dashboard.", style={'textAlign': 'center'}),
    # Main container
    html.Div([
        # Table container (left side)
        html.Div([
            dash_table.DataTable(
                id="country_table",
                columns=columns,
                data=display_data,
                sort_action="native",
                row_selectable="single",
                style_table={
                    "overflowX": "auto",
                    "height": "75vh",
                    "overflowY": "auto"
                },
                page_size=15,
                style_cell={
                    'textAlign': 'center',
                    'padding': '8px',
                    'whiteSpace': 'normal',
                    'height': 'auto'
                },
                style_header={
                    'backgroundColor': 'lightgrey',
                    'fontWeight': 'bold',
                    'position': 'sticky',
                    'top': 0
                },
                style_data={
                    'border': '1px solid lightgrey'
                }
            ),
        ], style={
            'width': '65%',
            'display': 'inline-block',
            'verticalAlign': 'top',
            'padding': '10px'
        }),
        # Side container (right side)
        html.Div([
            # Region filter container
            html.Div([
                html.H3("Filters", style={
                    "marginBottom": "20px",
                    'borderBottom': '1px solid #ddd',
                    'paddingBottom': '10px'
                }),
                html.Label("Filter by Region", style={'fontWeight': 'bold'}),
                dcc.RadioItems(
                    id="region-filter",
                    options=[{"label": "All Regions", "value": "all"}] + [{"label": region, "value": region} for region in regions],
                    value="all",
                    labelStyle={'display': 'block', 'margin': '5px 0'},
                    style={"marginBottom":"20px"}
                )
        ], style={
                'padding': '20px',
                'backgroundColor': '#f8f9fa',
                'borderRadius': '8px'
            }),
            # Flag section
            html.Div(id="flag_container", style={
                "marginTop": "20px",
                "padding": "20px",
                "textAlign": "center",
                "backgroundColor": "#f8f9fa",
                "borderRadius": "8px"
            })
        ], style={
            'width': '30%',
            'display': 'inline-block',
            'verticalAlign': 'top',
        })
    ], style={
        "width": "95%",
        "margin": "0 auto"
    })
])


@app.callback(
    Output("country_table", "data"),
    Input("region-filter", "value")
)
def update_table(selected_region: str) -> List[Dict]:
    """
    Update table based on selected region
    """
    filtered_data = display_data.copy()
    if selected_region != "all":
        filtered_data = [row for row in filtered_data if row["Region"] == selected_region]

    return filtered_data

@app.callback(
    Output("flag_container", "children"),
    Input("country_table", "selected_rows")
)

def update_flag(selected_rows: Optional[List[int]]) -> Component:
    """
    Displays the flag of the selected country.

    Returns:
        A dash component - a flag or a placeholder. 
    """

    if not selected_rows or len(selected_rows) == 0:
        return html.Div("Select a country to see its flag.")

    try:
        selected_country: Dict[str, Any] = display_data[selected_rows[0]]

        return html.Div([
            html.H3(
                f"Flag of {selected_country['Country Name']}.",
                style={"textAlign": "center"}
            ),
            html.Img(
                src=selected_country["flag_url"],
                style={
                    "height": "150px",
                    "border": "1px solid black",
                    "display": "block",
                    "margin": "0 auto"
                },
                alt=f"Flag of {selected_country['Country Name']}."
            ),
            html.P(
                f"Capital: {selected_country['Capital']} | Region: {selected_country['Region']}",
                style={"textAlign": "center"}
            )
        ])
    except (IndexError, KeyError) as error:
        return html.Div("Error loading country data.", style={"color": "red"})
    

# launch app
if __name__ == "__main__":
    app.run(debug=True)