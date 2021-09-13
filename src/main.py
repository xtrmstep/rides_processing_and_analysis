import processing


def main():

    processing.WeatherProcessor("./data/", "New York", "./data/processing/").process()


if __name__ == "__main__":
    main()
