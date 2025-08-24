# Function for getting data from an API and parsing it into a DataFrame:

def get_hourly_weather_data(row):

    start_date = '2023-01-01'
    end_date = '2024-01-02'
    features = 'temperature_2m,precipitation,snowfall,snow_depth,weather_code,visibility,wind_speed_10m,wind_direction_10m,wind_gusts_10m'
    units = 'temperature_unit=fahrenheit&wind_speed_unit=mph&precipitation_unit=inch'

    latitude = row['LATITUDE']
    longitude = row['LONGITUDE']
    airport = row['AIRPORT']
    airport_name = row['DISPLAY_AIRPORT_NAME']
    airport_id = row['AIRPORT_ID']

    response = requests.get(f'https://historical-forecast-api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}8&start_date={start_date}&end_date={end_date}&hourly={features}&{units}')

    # Check if API call successfull and execute response
    if response.status_code == 200:

        data = response.text
        parse_json = json.loads(data)


        # Extract metadata by stopping at 'hourly'
        metadata = {}
        for key, value in parse_json.items():
            if key == 'hourly':
                break
            metadata[key] = value

        # Save metadata as JSON
        with open(DATA_PATH + f'/interim/hourly_weather_data/{airport}_metadata.json', 'w') as json_file:
            json.dump(metadata, json_file, indent=4)

        # Process hourly data
        hourly_data = parse_json['hourly']

        # Create a DataFrame from the extracted hourly data
        weather_df = pd.DataFrame({
            'time': hourly_data['time'],
            'temperature_2m': hourly_data['temperature_2m'],
            'precipitation': hourly_data['precipitation'],
            'snowfall': hourly_data['snowfall'],
            'snow_depth': hourly_data['snow_depth'],
            'weather_code': hourly_data['weather_code'],
            'visibility': hourly_data['visibility'],
            'wind_speed_10m': hourly_data['wind_speed_10m'],
            'wind_direction_10m': hourly_data['wind_direction_10m'],
            'wind_gusts_10m': hourly_data['wind_gusts_10m']
        })

        # Convert the 'time' column to datetime format
        weather_df['time'] = pd.to_datetime(weather_df['time'])

        # Add airport identifier
        weather_df['airport'] = airport
        weather_df['airport_id'] = airport_id

        # Save dataframe as csv file
        weather_df.to_csv(DATA_PATH + f'/interim/hourly_weather_data/{airport}_weather.csv', index=False)
        print(f'Hourly weather data was saved for {airport_name}')
    
    else:
        print(f'Failed to retrieve data for {airport_name}. Status code: {response.status_code}')