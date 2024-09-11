#DO NOT DELETE YET THIS IS FOR REFERENCE !
#in app.py -> 
#from routes.data_routes import incident_blueprint, erp_blueprint, speedbands_blueprint, image_blueprint, vms_blueprint
#server.register_blueprint(incident_blueprint)
#server.register_blueprint(vms_blueprint)
#server.register_blueprint(erp_blueprint)
#server.register_blueprint(image_blueprint)
#server.register_blueprint(speedbands_blueprint)

##############################################################################################

from flask import Blueprint, render_template
from postgresql.db_functions import fetch_data_from_table
import folium

# Define the blueprint for Incident routes
incident_blueprint = Blueprint('incident_blueprint', __name__)

@incident_blueprint.route('/incidents')
def show_incidents():
    """Display incidents data and a map."""
    try:
        # Fetch data from the incident_table
        data = fetch_data_from_table('incident_table')
    except Exception as e:
        # Log the error and provide user feedback
        print(f"Error fetching Incident data: {e}")
        data = []  # In case of error, show an empty table

    # Define the columns for display
    columns = ['type', 'latitude', 'longitude', 'message']
    
    # Create a Folium map centered on Singapore (or another relevant location)
    map_obj = folium.Map(location=[1.3521, 103.8198], zoom_start=12)

    # Add markers dynamically from incidents data
    for row in data:
        folium.Marker(
            [row['latitude'], row['longitude']], 
            popup=row['message']
        ).add_to(map_obj)

    # Render map as HTML
    map_html = map_obj._repr_html_()

    # Render the template with the data and map
    return render_template('liveTraffic.html', table_name='Incident Table', data=data, columns=columns, map_html=map_html)

# Define the blueprint for ERP routes
erp_blueprint = Blueprint('erp_blueprint', __name__)

@erp_blueprint.route('/erp')
def show_erp():
    """Display ERP data."""
    try:
        # Fetch data from the erp_table
        data = fetch_data_from_table('erp_table')
    except Exception as e:
        # Log the error and provide user feedback
        print(f"Error fetching ERP data: {e}")
        data = []  # In case of error, show an empty table

    # Define the columns for display
    columns = ['vehicletype', 'daytype', 'starttime', 'endtime', 'zoneid', 'chargeamount', 'effectivedate']
    
    # Render the template with the data
    return render_template('liveTraffic.html', table_name='ERP Table', data=data, columns=columns)

# Define the blueprint for Speedbands routes
speedbands_blueprint = Blueprint('speedbands_blueprint', __name__)

@speedbands_blueprint.route('/speedbands')
def show_speedbands():
    """Display Speedbands data."""
    try:
        # Fetch data from the speedbands_table
        data = fetch_data_from_table('speedbands_table')
    except Exception as e:
        # Log the error and provide user feedback
        print(f"Error fetching Speedbands data: {e}")
        data = []  # In case of error, show an empty table

    # Define the columns for display
    columns = ['linkid', 'roadname', 'roadcategory', 'speedband', 'minimumspeed', 'maximumspeed', 'startlon']
    
    # Render the template with the data
    return render_template('liveTraffic.html', table_name='Speedbands Table', data=data, columns=columns)

# Define the blueprint for Traffic image routes
image_blueprint = Blueprint('image_blueprint', __name__)

@image_blueprint.route('/images')
def show_images():
    """Display Traffic Image data."""
    try:
        # Fetch data from the image_table
        data = fetch_data_from_table('image_table')
    except Exception as e:
        # Log the error and provide user feedback
        print(f"Error fetching Image data: {e}")
        data = []  # In case of error, show an empty table

    # Define the columns for display
    columns = ['cameraid', 'latitude', 'longitude', 'imagelink']
    
    # Render the template with the data
    return render_template('liveTraffic.html', table_name='Image Table', data=data, columns=columns)

# Define the blueprint for VMS routes
vms_blueprint = Blueprint('vms_blueprint', __name__)

@vms_blueprint.route('/vms')
def show_vms():
    """Display VMS data."""
    try:
        # Fetch data from the vms_table
        data = fetch_data_from_table('vms_table')
    except Exception as e:
        # Log the error and provide user feedback
        print(f"Error fetching VMS data: {e}")
        data = []  # In case of error, show an empty table

    # Define the columns for display
    columns = ['equipmentid', 'latitude', 'longitude', 'message']
    
    # Render the template with the data
    return render_template('liveTraffic.html', table_name='VMS Table', data=data, columns=columns)
