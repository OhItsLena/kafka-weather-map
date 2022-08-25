from datetime import datetime
import pandas as pd
import os
import plotly.express as px
from Kafka import KafkaConfluentReader
from IPython.display import clear_output

from dotenv import load_dotenv

# custom class to build plotly scattermapbox graphs based on forecast data from Kafka
class InfoMap:
    df_map_data = None  # data to plot
    df_locations = None  # locations with coordinates
    # consumer for cleaned topic (working with cleaned data)
    kafkaConfluentReader = None

    def __init__(self) -> None:
        load_dotenv()  # required to read env variables
        self.df_locations = self.get_locations_df()  # load locations.json
        token = os.getenv('MAPBOXKEY')  # get personal api key from .env file
        px.set_mapbox_access_token(token)
        self.kafkaConfluentReader = KafkaConfluentReader(
            'weather.cleaned', False)  # create consumer for cleaned topic, no auto commit

    # helper function to close consumer
    def close_kafka(self) -> None:
        self.kafkaConfluentReader.close()

    # get all messages from kafka, check for most recent key (collection time),
    # build and return dataframe with latest forecasts
    def get_forecasts_df(self) -> pd.DataFrame:
        df = pd.DataFrame(columns=['datetime', 'temp', 'city', 'advice'])
        messages = self.kafkaConfluentReader.get_all_messages(
            1.0)  # get all messages in a dictionary
        if messages:
            maxKey = pd.to_datetime(pd.DataFrame(messages.keys())[
                                    0]).max().strftime('%d.%m.%Y %H:%M')  # get dictionary key with latest collection time
            # for every city in most recent message
            for f in messages[maxKey]['cities']:
                city = f['city']
                for w in f['weather']:
                    dt = w['dt']
                    temp = w['temp']
                    # add some message for the user
                    adv = self.get_temp_advice(temp)
                    # add new row to dataframe
                    df.loc[len(df), df.columns] = dt, temp, city, adv
        return df

    # helper function to return a small message based on the temperature value
    def get_temp_advice(self, temp) -> str:
        if (temp >= 35):
            return "🥵   DANGER: <br>it's extremely hot! Stay hydrated."
        elif (temp >= 30):
            return "🥵   DANGER: <br>it's hot!"
        elif (temp >= 25):
            return "😰   CAUTION: <br>warm temperatures!"
        elif (temp >= 20):
            return "😊   INFO: <br>enjoy your day!"
        elif (temp >= 10):
            return "😕   INFO: <br>bring a jacket!"
        elif (temp >= 0):
            return "🥶   CAUTION: <br>it's cold outside!"
        else:
            return "🥶   DANGER: <br>it's freezing!"

    # read locations.json and return dataframe with city coordinates
    def get_locations_df(self) -> pd.DataFrame:
        df = pd.read_json('locations.json').T
        return df

    # put together forecast and coordinate information for map data
    def set_map_data(self, forecast) -> None:
        df = pd.merge(left=forecast, right=self.df_locations,
                      left_on='city', right_index=True)  # join forecast city on locations city
        df['temp'] = pd.to_numeric(df['temp'])
        # df['datetime'] = pd.to_datetime(df['datetime']) # plotly slider actually needs a string to work with ..
        if not df.empty:
            self.df_map_data = df

    # plot a plotly express scattermapbox graph to vizualise temperature forecasts
    def plot_map(self) -> None:
        # initial scatter_mapbox plot - slider through animation_frame property
        fig = px.scatter_mapbox(self.df_map_data, lat="latitude", lon="longitude", text="city", color="temp", width=1280, height=720, title="🌡️  Temperature Info Map", zoom=5,
                                color_continuous_scale=px.colors.cyclical.IceFire, range_color=(-10, 40), animation_frame='datetime', animation_group='city', custom_data=['temp', 'advice'])
        # add custom hovertemplate with more information
        fig.update_traces(hovertemplate="<b>%{text} </b><br>" +
                          "Temperature: %{customdata[0]} °C<br>" +
                          "<extra><i>%{customdata[1]}</i></extra>")  # needed to apply custom hovertemplate for first animation frame
        for f in fig.frames:  # needed to apply custom hovertemplate for every animation frame
            f.data[0].update(hovertemplate="<b>%{text} </b><br>" +
                             "Temperature: %{customdata[0]} °C<br>" +
                             "<extra><i>%{customdata[1]}</i></extra>")
        # set a fixed marker size
        fig.update_traces(marker={'size': 20, 'opacity': 0.75})

        # format text of the colorbar legend
        fig.update_coloraxes(colorbar_title_text="Temperature", colorbar_title_side="top",
                             colorbar_ypad=20, colorbar_tickformat=".1f", colorbar_ticksuffix=" °C")

        # adjust map margins to waste less space
        fig['layout']['margin'] = dict(r=0, t=50, l=20, b=0)

        # add a different prefix for the slider current value
        fig['layout']['sliders'][0]['currentvalue'] = {
            "prefix": "Date and time: "}

        # adjust slider margins to waste less space
        fig['layout']['sliders'][0]['pad'] = dict(r=0, t=10, l=0, b=20)

        # remove buttons to play animation
        fig["layout"].pop("updatemenus")

        # print the figure
        fig.show(renderer='vscode')  # vs code
        # fig.show(renderer='iframe') # jupyterlab

    # plot a map and keep forecasts in sync with most recent information
    def synced_map(self) -> None:
        df = pd.DataFrame(columns=['datetime', 'temp', 'city', 'advice'])
        try:
            print("Map Synchronisation started, waiting for new data ...")
            while True:
                msg = self.kafkaConfluentReader.poll(
                    1.0)  # get message a current offset
                currentKey = None
                if msg is not None:  # new message in cleaned topic found
                    # convert key to a datetime timestamp
                    newKey = datetime.strptime(msg.key(), '%d.%m.%Y %H:%M')
                    # new map if there is no map yet or if the map is outdated (older collection time - key)
                    if not currentKey or currentKey < newKey:
                        currentKey = newKey  # set current key to new collection time
                        for f in msg.value()['cities']:
                            city = f['city']
                            for w in f['weather']:
                                dt = w['dt']
                                temp = w['temp']
                                adv = self.get_temp_advice(temp)
                                df.loc[len(df),
                                       df.columns] = dt, temp, city, adv  # add row to dataframe
                        self.set_map_data(df)  # set map data to new dataframe
                        # clear current jupyter notebook output
                        clear_output(False)
                        self.plot_map()  # plot updated map
        except KeyboardInterrupt:  # stop sync with KeyboardInterrupt
            clear_output(True)  # remove map and inform user
            print("... Map Synchronisation stopped!")

    # plot a static map with most recent forecast (for manual update map) TASK 5: Infograph
    def update_map(self) -> None:
        forecasts = self.get_forecasts_df()  # get latest forecasts
        if not forecasts.empty:
            self.set_map_data(forecasts)  # set map data to latest forecasts
            self.plot_map()  # plot new map based on updated data
        else:  # something went wrong when accessing forecast data
            print("No information available to plot yet!")
