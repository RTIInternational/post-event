# teehr-post-event
A collection of modules and notebooks to facilitate interactive, visual evaluation of hydrologic forecasts for individual flood events.  

Installation instructions for three different ways of working with the post-event notebooks are described:  1) working remotely in the TEEHR Hub 2) working locally within a python virtual environment  3) working locally in a Docker container

## 1) Working remotely on the TEEHR Hub

Request to be added as a TEEHR Hub user:
- instructions TBD
  
Login to the TEEHR Hub from any browser: 
- Type ```teehr-hub.rtiamanzi.org``` in the address bar
- Click ```Sign in with GitHub```
- Follow instructions to log in with your GitHub credentials
- Select one of the two server options based on expected CPU needs
- JupyterLab will open once the server starts up (it may take a minute)

Open a terminal window in JupyterLab
- Click on the blue ```+``` button in the upper left corner to start a new Launcher (if a Launcher is not already open) 
- Start a terminal window by double clicking on the black square with "$_" symbol under the "Other" category

In the terminal, create a base working directory in which to clone the teehr-post-event repo and navigate into it:
```bash
$ mkdir ~/my_working_dir  
$ cd my_working_dir
```
Clone the teehr-post-event repo
```bash
$ git clone https://github.com/RTIInternational/teehr-post-event.git
```
Open a teehr-post-event notebook and begin working:
- Open the JupyterLab File Browser by clicking on the file symbol in the far left vertical toolbar
- Navigate into the repo directory in the File Browser panel (i.e. ~my_working_dir/teehr-post-event/)
- Double click on the "notebooks" directory
- Double click on the notebook you want to run to open it
- Follow instructions in the notebook


## 2) Working locally within a virtual environment

Create a working directory and change into it:  
```bash
$ mkdir ~/my_working_dir  
$ cd my_working_dir
```
Clone the teehr-post-event repo and change into the repo directory:
```bash
$ git clone https://github.com/RTIInternational/teehr-post-event.git
cd teehr-post-event
```
Copy the sample configuration file:
```bash
$ cd teehr-post-event/config
$ cp sample.json local_config.json
```
Edit the contents of the config file with your local root data directory, e.g.:
```bash
{
    "CACHE_ROOT": "C:/my_data_dir/post-event/"
}
```
Create your root data directory and geometry subdirectory:
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
Open a teehr-post-event notebook and begin working:
- Open the JupyterLab File Browser by clicking on the file symbol in the far left vertical toolbar
- Navigate into the repo directory in the File Browser panel (i.e. ~my_working_dir/teehr-post-event/)
- Double click on the "notebooks" directory
- Double click on the notebook you want to run to open it
- Follow instructions in the notebook

## 3) Working locally using Docker
Following steps above up to and including downloading geometry data.

Return to the working directory, clone the teehr repo, and change into the teehr directory:
```bash
cd my_working_dir
$ git clone https://github.com/RTIInternational/teehr.git
cd teehr
```
Switch to TEEHR v0.1.3
```bash
git checkout v0.1.3
```
Launch docker and mount your home directory
```bash
$ docker build -t teehr:v0.1.3 .
$ docker run -it --rm --volume $HOME:$HOME -p 8888:8888 teehr:v0.1.3 jupyter lab --ip 0.0.0.0 $HOME
```
