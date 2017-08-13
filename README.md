# Trading Portfolio Construction

## Dependencies (main module)
- mysql-connector (2.1.4)/ mysql-python (mysqldb)
- numpy
- pandas (0.20.0+)
- sklearn
- sqlalchemy
- tensorflow (1.0.1)

## Dependencies (crawler module)
- blpapi (3.9.0)
(can be installed by `conda install -c macinv blpapi` on Windows (64 bits only on python 2.7))
- futures
- mysql-connector (2.1.4)/ mysql-python (mysqldb)
- pandas (0.20.0+)
- schedule
- sqlalchemy
- tia
- tqdm

## Building the Main Module
1. Clone the repository by issuing the command

   `git clone https://github.com/clarkwkw/ml_stocks_platform.git`

2. Change directory to the repository folder and run the bash script by issuing

   `sh build.sh`

3. Enter the path of Python 2 interpreter, leave it blank if you wish to use the default one
4. The Python scripts are packed into a signle executable `mlsp`
5. Move the executable to any folder you wish it to be stored
6. (Optional) Add the folder containing the executable into $PATH variable by issuing

   `export PATH=$PATH:$(pwd)`

## Basic Usage
1. Change working directory to an empty folder

2. Prepare the config files by issuing the commands

   `mlsp -g simulation`

   `mlsp -g stock_data`

   Please follow instruction shown in the program to configure the parameters.

3. Modify `simulation_config.json` if you wish to perform meta-parameter selection or to pass parameters to the model

4. Start simulation by issuing `mlsp`

## Documentation
Please refer to the [wikipage](https://github.com/clarkwkw/summer_research/wiki).
