from xero_dash import app

if __name__ == "__main__":
    # host="0.0.0.0" is needed for Docker-compose to work
    app.run_server(debug=True, host="0.0.0.0")
