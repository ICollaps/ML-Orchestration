import streamlit as st
from streamlit.components.v1 import html

import streamlit_shadcn_ui as ui

import ydata_profiling
from streamlit_pandas_profiling import st_profile_report
from ydata_profiling import ProfileReport

import pandas as pd
from pygwalker.api.streamlit import StreamlitRenderer

import clickhouse_connect
import pydeck as pdk
from fonction import getRoute, getVelibStationWithinXm, geocode, getRoute2
import requests
import json
from bs4 import BeautifulSoup
import re
from PIL import Image

st.set_page_config(layout="wide")

client = clickhouse_connect.get_client(host='127.0.0.1')


df = client.query('select * from velib')

columns = ['id', 'stationCode', 'station_id', 'num_bikes_available', 'mechanical', 'ebike', 'num_docks_available','is_installed', 'is_returning', 'is_renting', 'last_reported', 'weather', 'temp', 'probarain','probafog', 'probawind70', 'probawind100', 'lon', 'lat', 'name']

    # Créer un DataFrame
df = pd.DataFrame(df.result_rows, columns=columns)

# ajouter une sidebar avec 4 pages (Dashboard, Analyses, Prédiction et A propos)

image_path = 'velib.png'
image = Image.open(image_path)

st.sidebar.image(image)
st.sidebar.title("Navigation")

page = st.sidebar.radio("Go to", ["A propos", "Dashboard", "Analyses", "Prédiction"])

predictions = [[0,0,0,0,0],[0,0,0,0,0],[0,0,0,0,0]]

if page == "Dashboard":
    st.header("Dashboard")
    ui.badges(badge_list=[("shadcn", "default"), ("in", "secondary"), ("streamlit", "destructive")], class_name="flex gap-2", key="main_badges1")

    # Mostrar la imagen original
    st.image(image)

    st.caption("A Streamlit component library for building beautiful apps easily. Bring the power of Shadcn UI to your Streamlit apps!")

    cols = st.columns(3)
    with cols[0]:
        ui.card(title="Place de la République - Tample", content="12 Docks availables", description="5 Mechanical, 7 ebike", key="card1").render()
    with cols[1]:
        ui.card(title="Subscriptions", content="+2350", description="+180.1% from last month", key="card2").render()
    with cols[2]:
        ui.card(title="Sales", content="+12,234", description="+19% from last month", key="card3").render()

    # df = pd.read_csv("080523-2days-correct.csv")
    pyg_app = StreamlitRenderer(df)
    pyg_app.explorer()

elif page == "Analyses":
    st.header("Analyses")
    ui.badges(badge_list=[("shadcn", "default"), ("in", "secondary"), ("streamlit", "destructive")], class_name="flex gap-2", key="main_badges1")
    st.caption("A Streamlit component library for building beautiful apps easily. Bring the power of Shadcn UI to your Streamlit apps!")

    # df = pd.read_csv("080523-2days-correct.csv")
    pr = ProfileReport(df, title="Test Exploration", explorative=True)

    st_profile_report(pr)

elif page == "Prédiction":
    st.header("Prédiction")
    ui.badges(badge_list=[("shadcn", "default"), ("in", "secondary"), ("streamlit", "destructive")], class_name="flex gap-2", key="main_badges1")
    st.caption("A Streamlit component library for building beautiful apps easily. Bring the power of Shadcn UI to your Streamlit apps!")

    # Ajout des entrées texte
    start_point = st.text_input("Start point", placeholder="Start point...")
    end_point = st.text_input("End point", placeholder="End point...")

    path = [[48.86725, 2.36447]]

    # Ajout du bouton Predict
    if st.button("Predict"):
        # Appel de votre fonction de prédiction avec les valeurs des entrées texte
        
        path = getRoute(start_point, end_point)
        data = getVelibStationWithinXm(geocode(end_point), 200)

        #st.write(data)
        lenght_data = len(data)
        if lenght_data>3:
            lenght_data=3

        predictions = []
        for i in range(lenght_data):
                

                # data_info= data[i]


                # data_infos = getRoute2(start_point, end_point)

                # # Clés à garder pour l'API
                # keys_to_keep = [
                #     'num_bikes_available', 'mechanical', 'ebike',
                #     'is_installed', 'is_returning', 'is_renting',
                #     'last_reported', 'weather', 'temp', 'probarain', 'probafog', 'probawind70', 'probawind100','lon','lat'
                # ]

                # # Filtrer les données pour ne garder que les clés nécessaires
                # filtered_data = {key: data_info[key] for key in keys_to_keep}

                data_info = data[i]

                data_infos = getRoute2(start_point, end_point)

                # Clés à garder pour l'API
                keys_to_keep = [
                    'num_bikes_available', 'mechanical', 'ebike',
                    'is_installed', 'is_returning', 'is_renting',
                    'last_reported', 'weather', 'temp', 'probarain', 'probafog', 'probawind70', 'probawind100'
                ]

                # Filtrer les données pour ne garder que les clés nécessaires
                filtered_data = {key: data_info[key] for key in keys_to_keep}

                # Renommer les clés longitude en lon et latitude en lat
                if 'longitude' in data_info:
                    filtered_data['lon'] = int(data_info['longitude'])
                if 'latitude' in data_info:
                    filtered_data['lat'] = int(data_info['latitude'])

                print(filtered_data)

                # URL de l'API
                url = 'http://127.0.0.1:8000/predict'

                # Envoyer la requête POST
                response = requests.post(url, data=json.dumps(filtered_data), headers={'Content-Type': 'application/json'})

                distance = data_info["distance"]
                predict = response.json()
                predict = predict["prediction"]


                predictions.append([data_info["name"],int(predict[0]),int(distance),data_info["longitude"],data_info["latitude"]])

        cols = st.columns(lenght_data)

        with cols[0]:
            ui.card(title=predictions[0][0], content=f"Docks: {predictions[0][1]}",
                    description=f"Distance: {predictions[0][2]}m",
                    key="card" + str(11)).render()
        if lenght_data>1:
            with cols[1]:
                ui.card(title=predictions[1][0], content=f"Docks: {predictions[1][1]}", description=f"Distance: {predictions[1][2]}m",
                    key="card"+str(12)).render()
            if lenght_data>2:
                with cols[2]:
                    ui.card(title=predictions[2][0], content=f"Docks: {predictions[2][1]}", description=f"Distance: {predictions[2][2]}m",
                        key="card"+str(13)).render()

        # Les données fournies
        data = {
            'distance': ['34 m', '37 m', '0.1 km', '16 m', '25 m', '32 m', '45 m', '0.1 km', '0.5 km', '0.6 km', '0.3 km', '0.3 km', '0.2 km', '0.1 km', '0.2 km', '77 m', '0.8 km', '0.7 km', '0.3 km', '0.2 km', '92 m', '0.1 km'],
            'duration': ['1 min', '1 min', '1 min', '1 min', '1 min', '1 min', '1 min', '1 min', '1 min', '3 mins', '1 min', '1 min', '1 min', '1 min', '1 min', '1 min', '5 mins', '3 mins', '1 min', '1 min', '1 min', '1 min'],
            'html_instructions': ['Head <b>northwest</b> on <b>Pl. de la République</b>', 'Turn <b>right</b> to stay on <b>Pl. de la République</b>', 'Turn <b>right</b> toward <b>Pl. de la République</b>', 'Turn <b>right</b> toward <b>Pl. de la République</b>', 'Turn <b>left</b> toward <b>Pl. de la République</b>', 'Turn <b>right</b> toward <b>Pl. de la République</b>', 'Turn <b>right</b> onto <b>Pl. de la République</b>', 'Turn <b>left</b> onto <b>Rue du Temple</b>', 'Continue onto <b>R. de Turbigo</b>', 'Turn <b>left</b> onto <b>Rue Beaubourg</b>', 'Continue onto <b>Rue du Renard</b>', 'Turn <b>right</b> onto <b>Rue de Rivoli</b>', 'Turn <b>left</b> onto <b>Bd de Sébastopol</b>/<wbr/><b>Traversée N S</b><div style="font-size:0.9em">Continue to follow Traversée N S</div>', 'Continue onto <b>Pont au Change</b>', 'Continue onto <b>Bd du Palais</b>', 'Continue onto <b>Pont Saint-Michel</b>', 'Continue onto <b>Pl. Saint-Michel</b>', 'Take the crosswalk', "Continue onto <b>Av. de l'Observatoire</b>", 'Continue onto <b>Av. Denfert Rochereau</b>', 'Turn <b>right</b>', 'Turn <b>left</b><div style="font-size:0.9em">Destination will be on the right</div>']
        }

        def parse_distance(distance_str):
            """Convert distance string to meters"""
            if 'km' in distance_str:
                return float(distance_str.replace(' km', '')) * 1000
            elif 'm' in distance_str:
                return float(distance_str.replace(' m', ''))
            else:
                return 0

        def parse_duration(duration_str):
            """Convert duration string to minutes"""
            match = re.match(r"(\d+)\s*min", duration_str)
            if match:
                return int(match.group(1))
            else:
                return 0

        # Calculer le total des distances en mètres
        total_distance = sum(parse_distance(dist) for dist in data_infos['distance'])
        total_distance = round((total_distance / 1000), 2)

        # Calculer le total des durées en minutes
        total_duration = sum(parse_duration(dur) for dur in data_infos['duration'])

        # Fonction pour supprimer les balises HTML
        def remove_html_tags(text):
            soup = BeautifulSoup(text, "html.parser")
            return soup.get_text()

        # Nettoyer les instructions HTML
        data_infos['instructions'] = [remove_html_tags(instr) for instr in data_infos['html_instructions']]

        # Créer le DataFrame
        df = pd.DataFrame({
            'distance': data_infos['distance'],
            'duration': data_infos['duration'],
            'instruction': data_infos['instructions']
        })

        st.write("## Directions")


        # Afficher le DataFrame avec Streamlit
        cols = st.columns(3)
        with cols[0]:
            ui.card(title="Travel time", content=f"{total_duration} mins", description="(Depending on your speed)", key="card4").render()
        with cols[1]:
            ui.card(title="Distance", content=f"{total_distance} km", description="(Based on the fastest way)", key="card5").render()
        with cols[2]:
            ui.card(title="Instructions", content=f"{data_infos['length']} Steps", description="(From start to end point)", key="card6").render()

        ui.table(data=df, maxHeight=300)

    # Conversion des coordonnées en format compatible avec pydeck
    path_coordinates = [{"path": [[lon, lat] for lat, lon in path]}]

    # Création de la couche TripsLayer
    trips_layer = pdk.Layer(
        "TripsLayer",
        path_coordinates,
        get_path="path",
        get_color=[255, 255, 255],
        width_min_pixels=5,
        rounded=True,
        trail_length=600,
        current_time=0
    )
    longitude1 = predictions[0][3]
    latitude1 = predictions[0][4]

    predict1 = pdk.Layer(
        "ScatterplotLayer",
        predictions,
        pickable=True,
        opacity=0.2,
        stroked=True,
        filled=True,
        radius_scale=6,
        radius_min_pixels=5,
        radius_max_pixels=10,
        line_width_min_pixels=1,
        get_position=[longitude1,latitude1],
        get_radius=predictions[0][1],
        get_fill_color=[255, 255, 255],
        get_line_color=[0, 0, 0],
        )

    if len(predictions)>1:
        longitude2 = predictions[1][3]
        latitude2 = predictions[1][4]

        predict2 = pdk.Layer(
            "ScatterplotLayer",
            predictions,
            pickable=True,
            opacity=0.2,
            stroked=True,
            filled=True,
            radius_scale=6,
            radius_min_pixels=5,
            radius_max_pixels=10,
            line_width_min_pixels=1,
            get_position=[longitude2, latitude2],
            get_radius=predictions[1][1],
            get_fill_color=[255, 255, 255],
            get_line_color=[0, 0, 0],
        )

        if len(predictions) > 2:
            longitude3 = predictions[2][3]
            latitude3 = predictions[2][4]

            predict3 = pdk.Layer(
                "ScatterplotLayer",
                predictions,
                pickable=True,
                opacity=0.2,
                stroked=True,
                filled=True,
                radius_scale=6,
                radius_min_pixels=5,
                radius_max_pixels=10,
                line_width_min_pixels=1,
                get_position=[longitude3, latitude3],
                get_radius=predictions[2][1],
                get_fill_color=[255, 255, 255],
                get_line_color=[0, 0, 0],
            )

    # Vue initiale centrée sur le premier point du chemin
    view_state = pdk.ViewState(
        latitude=48.854414,
        longitude= 2.347548,
        zoom=13,
        pitch=50
    )


    # Création de la carte
    if len(predictions) == 1:
        r = pdk.Deck(layers=[trips_layer, predict1], initial_view_state=view_state)
    if len(predictions)==2:
        r = pdk.Deck(layers=[trips_layer, predict1,predict2], initial_view_state=view_state)
    if len(predictions)==3:
        r = pdk.Deck(layers=[trips_layer, predict1,predict2,predict3], initial_view_state=view_state)

    st.pydeck_chart(r)



elif page == "A propos":
    st.markdown('<div class="full-width">', unsafe_allow_html=True)
    st.header("A propos")
    st.caption("This application have been built in order to create a beautiful Streamlit app using Shadcn UI. The main goal is to analyze and predict the amout of Vélib available in Paris based on a MLP model trained by our own.")
    st.write("### Stack Technique")
    ui.badges(badge_list=[("Python", "default"), ("Airflow", "default"), ("ClickHouse", "default"), ("Mimio", "default"), ("Redis", "default"),("Pandas", "secondary"), ("Streamlit", "secondary"), ("shadcn", "secondary"), ("Ydata", "secondary")], class_name="flex gap-2", key="main_badges2")

    # Afficher les informations des participants
    st.write("### Participant Information")

    cols = st.columns(2)

    with cols[0]:

        with ui.card(key=f"Barbot"):
                ui.element("img",
                    src="https://avatars.githubusercontent.com/u/75270400?v=4",
                    className="rounded-full h-12 w-12 justify-center items-center",
                )

                ui.element("span", children=["Identité"], className="text-gray-400 text-sm font-medium m-1", key="label1")
                ui.element("input", key=f"email", value="Allan BARBOT")

                ui.element("span", children=["Société"], className="text-gray-400 text-sm font-medium m-1", key="label2")
                ui.element("input", key="test", value="Manutan France")

                ui.element("span", children=["Poste"], className="text-gray-400 text-sm font-medium m-1", key="label3")
                ui.element("input", key="test1", value="AI Engineer / ETL Developer")

                ui.element("button", text="Mail", key="button", className="m-2")
                # Bouton pour rediriger vers GitHu
                ui.element("link_button", text="Go To Github", url="https://github.com/CurtainShow", key="link_btn")

        with ui.card(key=f"Mame"):
                ui.element("img",
                    src="https://avatars.githubusercontent.com/u/55426379?v=4",
                    className="rounded-full h-12 w-12 justify-center items-center",
                )

                ui.element("span", children=["Identité"], className="text-gray-400 text-sm font-medium m-1", key="label4")
                ui.element("input", key=f"email", value="Djbril Mame")

                ui.element("span", children=["Société"], className="text-gray-400 text-sm font-medium m-1", key="label5")
                ui.element("input", key="test", value="Kweet")

                ui.element("span", children=["Poste"], className="text-gray-400 text-sm font-medium m-1", key="label6")
                ui.element("input", key="test1", value="Data Engineer")

                ui.element("button", text="Mail", key="button", className="m-2")
                ui.element("link_button", text="Go To Github", url="https://github.com/djibygass", key="link_btn")

    with cols[1]:
        with ui.card(key=f"Collas"):
                ui.element("img",
                    src="https://avatars.githubusercontent.com/u/98404700?v=4",
                    className="rounded-full h-12 w-12 justify-center items-center",
                )

                ui.element("span", children=["Identité"], className="text-gray-400 text-sm font-medium m-1", key="label7")
                ui.element("input", key=f"email", value="Pierre Collas")

                ui.element("span", children=["Société"], className="text-gray-400 text-sm font-medium m-1", key="label8")
                ui.element("input", key="test", value="Scan Tech")

                ui.element("span", children=["Poste"], className="text-gray-400 text-sm font-medium m-1", key="label9")
                ui.element("input", key="test1", value="Data Engineer")

                ui.element("button", text="Mail", key="button", className="m-2")
                ui.element("link_button", text="Go To Github", url="https://github.com/ICollaps", key="link_btn")

        with ui.card(key=f"Rodriguez"):
                ui.element("img",
                    src="https://avatars.githubusercontent.com/u/90451351?v=4",
                    className="rounded-full h-12 w-12 justify-center items-center",
                )

                ui.element("span", children=["Identité"], className="text-gray-400 text-sm font-medium m-1", key="label10")
                ui.element("input", key=f"email", value="Adrian Barquero Rodriguez")

                ui.element("span", children=["Société"], className="text-gray-400 text-sm font-medium m-1", key="label11")
                ui.element("input", key="test", value="SNCF")

                ui.element("span", children=["Poste"], className="text-gray-400 text-sm font-medium m-1", key="label12")
                ui.element("input", key="test1", value="Data Engineer")

                ui.element("button", text="Mail", key="button", className="m-2")
                ui.element("link_button", text="Go To Github", url="https://github.com/brqan", key="link_btn")