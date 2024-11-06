from flask import Blueprint, render_template, send_from_directory
from dash import Dash

# Create a Blueprint for template-related routes
templates_blueprint = Blueprint('templates_blueprint', __name__)

# Route to serve static files
@templates_blueprint.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory('static', filename)

# Route to landing page
@templates_blueprint.route('/')
def serve_main():
    return render_template('main.html')

# Route for Dashboard, served by Dash
@templates_blueprint.route('/dashboard')
def redirect_to_dashboard():
    return Dash.__getattr__('index')(Dash)  # Ensure `app` from Dash is correctly called

@templates_blueprint.route('/traffic_overview')
def traffic_overview():

    return render_template('trafficOverview.html')

@templates_blueprint.route('/traffic_insight')
def traffic_insight():

    return render_template('trafficInsight.html')