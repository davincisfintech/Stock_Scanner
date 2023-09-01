***  Installation ***

install python 3.8 for your operating system using this link  https://www.python.org/downloads/

(optional)
install pycharm community edition using this link  https://www.jetbrains.com/pycharm/download/


*** Setup  ***

run following commands from command prompt/terminal

for MacOs/Linux:
      cd < project directory>           # move to project directory
      python -m venv venv              # create virtual environment
      source venv/bin/activate         # activate virtual environment
      pip install -r requirements.txt    # install dependencies

for windows:
       cd < project directory>           # move to project directory
       python -m venv venv              # create virtual environment
       venv\Scripts\activate            # activate virtual environment
       pip install -r requirements.txt     # install dependencies


*** API Details ***
add your polygon api inside config.json file in config folder as specified in it.


*** Parameters ***

provide your parameters in excel files in parameters folder as instructed in sheet named Instructions


*** How To Run ***

Option 1:
    To run using pycharm:

    set run.py located in main folder in pycharm configuration and click on run button to run program

Option 2:
    To run using terminal:

    run following commands from command prompt/terminal

    for MacOs/Linux:
          cd < project directory>           # move to project directory
          source venv/bin/activate         # activate virtual environment
          python run.py   # Run program

    for windows:
           cd < project directory>           # move to project directory
           venv\Scripts\activate            # activate virtual environment
           run.py           # Run program

Option 3:
    for windows OS only:
    double click run.bat file


*** Output file ***
output excel file containing results will get created inside records folder 


*** logs ***

Each run will create date wise log files inside logs folder showing all details