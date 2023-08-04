# teehr-post-event
A collection of modules and notebooks to facilitate interactive, visual evaluation of hydrologic forecasts for individual flood events

## Local installation and configuration

Create a working directory and change into it:  
```bash
$ mkdir my_working_dir  
$ cd my_working_dir
```

Clone the teehr-post-event repo:
```bash
$ git clone https://github.com/RTIInternational/teehr-post-event
$ cd teehr-post-event
```

Create and activate a python virtual environment with required packages installed:
```bash
Using conda and package_list.txt:
$ conda create --name ENV -- package_list.txt
$ conda activate ENV

Using pip:
$ python3 -m venv env
$ source env/bin/activate
$ python3 -m pip install --upgrade pip
$ ...?

```


Copy the sample configuration file:
```bash
$ cd config
$ cp sample.json post-event-config.json
```

Edit the contents of the config file with your root data directory, e.g.:
```bash
{
    "CACHE_ROOT": "C:/my_data_dir/post-event/"
}
```

Create your root data directory and geometry subdirectory:
```bash
$ mkdir my_data_dir/post-event/
$ mkdir my_data_dir/post-event/geo
```

Download necessary geometry files into your ```/geo``` directory: 
```bash
$ cd C:/my_data_dir/post-event/geo
$ wget https://ciroh-rti-public-data.s3.us-east-2.amazonaws.com/nwm-post-event-geometry-aug-2023.tar.gz
$ tar -xzvf nwm-post-event-geometry-aug-2023.tar.gz
```

Return to your working directory and launch jupyter:
```bash
$ cd my_working_dir
$ jupyter lab
```

