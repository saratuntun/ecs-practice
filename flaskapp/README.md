# group-project-f22-pulp-prediction

## Prediction Service
### Deploy and run the app
1. Connect to the VM in the SSH session.
2. Set up the Git repository on the VM.
3. Install the dependencies for Flask with following commands.
```
sudo apt install python3-flask
export FLASK_APP=flaskapp/app.py
```
4. Run the app with following commands.
``` 
flask run
```

## Testing
### Generate coverage report
To automatically generate the coverage report and open it in HTML file, run the following commands:
```
coverage run --source=<package> -m pytest -v tests && coverage html && open htmlcov/index.html
```
