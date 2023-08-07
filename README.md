# teehr-post-event
A collection of modules and notebooks to facilitate interactive, visual evaluation of hydrologic forecasts for individual flood events.  

Installation instructions for three different ways of working with the post-event notebooks are described below:  1) working remotely on the TEEHR Hub (AWS) 2) working locally within a python virtual environment  3) working locally in a Docker container



## 1) Working remotely on the TEEHR Hub
add instructions   

teehr-hub.rtiamanzi.org


## 2) Working locally within a virtual environment

Create a working directory and change into it:  
```bash
$ mkdir ~/my_working_dir  
$ cd my_working_dir
```
Clone the teehr-post-event repo and install lfs:
```bash
$ git clone https://github.com/RTIInternational/teehr-post-event
```
Copy the sample configuration file:
```bash
$ cd teehr-post-event/config
$ cp sample.json post-event-config.json
```
Edit the contents of the config file with your local root data directory, e.g.:
```bash
{
    "CACHE_ROOT": "C:/my_data_dir/post-event/"
}
```
Create your root data directory and geometry subdirectory :
```bash
$ mkdir C:/my_data_dir/post-event/
$ mkdir C:/my_data_dir/post-event/geo
```
Download necessary geometry files into your ```/geo``` directory: 
```bash
$ cd C:/my_data_dir/post-event/geo
$ wget https://ciroh-rti-public-data.s3.us-east-2.amazonaws.com/nwm_post_event_geometry_aug_2023.tar.gz -O nwm_post_event_geometry_aug_2023.tar.gz
$ tar -xzvf nwm_post_event_geometry_aug_2023.tar.gz
```
Create and activate a python virtual environment with required packages installed:
```bash
Using conda and package_list.txt:
$ conda create --name ENV --file package_list.txt
$ conda activate ENV
```
Navigate to your working directory and launch jupyter:
```bash
$ cd my_working_dir
$ jupyter lab
```
## 3) Working locally using Docker
Following steps above up to and including downloading geometry data.

clone teehr repo into it's own directory (go up to working dir level)
cd into teehr repo dir
git checkout v0.1.3

Launch docker and mount your home directory
```bash
$ docker build -t teehr:v0.1.3 .
$ docker run -it --rm --volume $HOME:$HOME -p 8888:8888 teehr:v0.1.3 jupyter lab --ip 0.0.0.0 $HOME
```
Navigate to your working directory and launch jupyter:
```bash
$ cd my_working_dir
$ jupyter lab
```
