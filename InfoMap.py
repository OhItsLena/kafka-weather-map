from datetime import datetime
import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go
from Kafka import KafkaConfluentReader
from IPython.display import clear_output

from dotenv import load_dotenv


class InfoMap:
    df_map_data = None
    df_locations = None
    kafkaConfluentReader = KafkaConfluentReader('weather.cleaned', False)

    def __init__(self) -> None:
        load_dotenv()
        self.df_locations = self.get_locations_df()
        token = os.getenv('MAPBOXKEY')
        px.set_mapbox_access_token(token)

    def close_kafka(self) -> None:
        self.kafkaConfluentReader.close()

    def get_forecasts_df(self) -> pd.DataFrame:
        df = pd.DataFrame(columns=['datetime', 'temp', 'city', 'advice'])
        messages = self.kafkaConfluentReader.get_all_messages(1.0)
        if messages:
            maxKey = pd.to_datetime(pd.DataFrame(messages.keys())[
                                    0]).max().strftime('%d.%m.%Y %H:%M')
            for f in messages[maxKey]['cities']:
                city = f['city']
                for w in f['weather']:
                    dt = w['dt']
                    temp = w['temp']
                    adv = self.get_temp_advice(temp)
                    df.loc[len(df), df.columns] = dt, temp, city, adv
        return df

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

    def get_locations_df(self) -> pd.DataFrame:
        df = pd.read_json('locations.json').T
        return df

    def set_map_data(self, forecast) -> None:
        df = pd.merge(left=forecast, right=self.df_locations,
                      left_on='city', right_index=True)
        df['temp'] = pd.to_numeric(df['temp'])
        #df['datetime'] = pd.to_datetime(df['datetime'])
        if not df.empty:
            self.df_map_data = df

    def plot_map(self) -> None:
        fig = px.scatter_mapbox(self.df_map_data, lat="latitude", lon="longitude", text="city", color="temp", width=1280, height=720, title="🌡️  Temperature Info Map",
                                color_continuous_scale=px.colors.cyclical.IceFire, range_color=(-10, 40), animation_frame='datetime', animation_group='city', custom_data=['temp', 'advice'])
        fig.update_traces(hovertemplate="<b>%{text} </b><br>" +
                          "Temperature: %{customdata[0]} °C<br>" +
                          "<extra><i>%{customdata[1]}</i></extra>")
        for f in fig.frames:
            f.data[0].update(hovertemplate="<b>%{text} </b><br>" +
                             "Temperature: %{customdata[0]} °C<br>" +
                             "<extra><i>%{customdata[1]}</i></extra>")
        fig.update_traces(marker={'size': 20, 'opacity': 0.75})
        fig.update_coloraxes(colorbar_title_text="Temperature", colorbar_title_side="top",
                             colorbar_ypad=20, colorbar_tickformat=".1f", colorbar_ticksuffix=" °C")
        fig['layout']['margin'] = dict(r=0, t=50, l=20, b=0)
        fig['layout']['sliders'][0]['currentvalue'] = {
            "prefix": "Date and time: "}
        fig['layout']['sliders'][0]['pad'] = dict(r=0, t=10, l=0, b=20)
        fig["layout"].pop("updatemenus")
        fig.show()

    def synced_map(self) -> None:
        df = pd.DataFrame(columns=['datetime', 'temp', 'city', 'advice'])
        try:
            print("Map Synchronisation started ...")
            while True:
                msg = self.kafkaConfluentReader.poll(1.0)
                currentKey = None
                if msg is not None:
                    newKey = datetime.strptime(msg.key(), '%d.%m.%Y %H:%M')
                    print(newKey)
                    if not currentKey or currentKey < msg.Key():
                        for f in msg.value()['cities']:
                            city = f['city']
                            for w in f['weather']:
                                dt = w['dt']
                                temp = w['temp']
                                adv = self.get_temp_advice(temp)
                                df.loc[len(df),
                                       df.columns] = dt, temp, city, adv
                        self.set_map_data(df)
                        clear_output(False)
                        self.plot_map()
        except KeyboardInterrupt:
            clear_output(True)
            print("... Map Synchronisation stopped!")

    def update_map(self) -> None:
        forecasts = self.get_forecasts_df()
        if not forecasts.empty:
            self.set_map_data(forecasts)
            self.plot_map()
        else:
            print("No available to plot yet!")
