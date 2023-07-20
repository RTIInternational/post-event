# post-event
A collection of modules and notebooks to facilitate exploratory evaluation of hydrologic forecast performance following individual flood events

## How to install post-event notebooks

Install from GitHub
```bash
$ pip install 'post-event @ git+https://github.com/RTIInternational/post-event@[BRANCH_TAG]'
```

## How to configure post-event notebooks for local execution

Create your root data directory, e.g:
```bash
C:/post-event/cache
```
Create your geometry directory, e.g:
```bash
C:/post-event/cache/geo
```
download necessary geometry and crosswalk files into your ```/geo``` directory: (figure this out)

Define your local cache root directory in post-event-config.json
